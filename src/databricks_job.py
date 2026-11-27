"""Build managed Delta tables from bounded PGN files in Databricks or local Spark."""

from __future__ import annotations

import argparse
import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from chess_lakehouse import __version__
from chess_lakehouse.lineage import batch_id, source_metadata


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-path", required=True, help="Volume path/glob containing PGN files"
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--source-url", default="")
    parser.add_argument("--requested-max-games", type=lambda value: int(value) if value else None)
    parser.add_argument("--expected-source-sha256", default="")
    parser.add_argument("--retrieved-at-utc", default="")
    return parser.parse_args()


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"unsafe catalog or schema identifier: {value!r}")
    return value


def _tag(name: str) -> F.Column:
    return F.regexp_extract("raw_pgn", rf'(?m)^\[{name} "([^"]*)"\]$', 1)


PROVENANCE_COLUMNS = [
    "source_path",
    "source_sha256",
    "source_url",
    "batch_id",
    "source_kind",
    "source_license",
    "source_license_url",
    "requested_max_games",
    "retrieved_at_utc",
    "pipeline_version",
]


def _source_files(
    spark, source_path, source_url=None, requested_max_games=None, retrieved_at_utc=None
):
    # The Spark path accepts bounded, decompressed PGN only, not monthly archives.
    files = (
        spark.read.format("binaryFile")
        .option("ignoreCorruptFiles", "false")
        .option("ignoreMissingFiles", "false")
        .load(source_path)
        .select(
            F.col("path").alias("source_path"),
            "content",
            F.sha2("content", 256).alias("source_sha256"),
        )
    )
    metadata = source_metadata(source_url or "")
    return (
        files.withColumn("source_url", F.lit(source_url) if source_url else F.col("source_path"))
        .withColumn("batch_id", F.sha2(F.concat_ws("\n", "source_url", "source_sha256"), 256))
        .withColumn("source_kind", F.lit(metadata["source_kind"]))
        .withColumn("source_license", F.lit(metadata["source_license"]).cast("string"))
        .withColumn("source_license_url", F.lit(metadata["source_license_url"]).cast("string"))
        .withColumn("requested_max_games", F.lit(requested_max_games).cast("long"))
        .withColumn("retrieved_at_utc", F.lit(retrieved_at_utc).cast("string"))
        .withColumn("pipeline_version", F.lit(__version__))
    )


def _file_receipts(files, source_url, requested_max_games, retrieved_at_utc):
    """Spark skips zero-byte binary files; explicitly verify and retain their receipts."""
    rows = files.select(*PROVENANCE_COLUMNS).collect()
    seen = {urlsplit(row.source_path) for row in rows}
    for path in files.inputFiles():
        parsed = urlsplit(path)
        if parsed in seen:
            continue
        # Do not infer an empty input from an omitted scan row without checking its size.
        if parsed.scheme not in ("", "file", "dbfs") or (
            parsed.scheme == "dbfs" and not parsed.path.startswith("/Volumes/")
        ):
            raise ValueError(f"Cannot verify omitted Spark input file: {path}")
        if Path(unquote(parsed.path)).stat().st_size != 0:
            raise ValueError(f"Spark omitted a nonempty input file: {path}")
        sha256 = hashlib.sha256(b"").hexdigest()
        url = source_url or path
        values = {
            "source_path": path,
            "source_sha256": sha256,
            "source_url": url,
            "batch_id": batch_id(url, sha256),
            **source_metadata(source_url or ""),
            "requested_max_games": requested_max_games,
            "retrieved_at_utc": retrieved_at_utc,
            "pipeline_version": __version__,
        }
        rows.append(Row(**{name: values[name] for name in PROVENANCE_COLUMNS}))
        seen.add(parsed)
    if not rows:
        raise ValueError("No input files were found; cannot establish batch lineage")
    return rows


def _raw_records(files: DataFrame) -> DataFrame:
    records = files.select(
        F.posexplode(
            F.split(
                F.regexp_replace(F.decode("content", "UTF-8"), r"\r\n", "\n"), r"(?m)(?=^\[Event )"
            )
        ).alias("position", "raw_pgn"),
        *PROVENANCE_COLUMNS,
    ).withColumn("raw_pgn", F.regexp_replace("raw_pgn", r"^\s+|\s+$", ""))
    records = records.where(F.col("raw_pgn") != "")
    site = _tag("Site")
    lichess_id = F.regexp_extract(
        site, r"^https?://lichess\.org/(?:game/)?([A-Za-z0-9]{8})(?:/.*)?$", 1
    )
    canonical = F.array_join(
        F.filter(
            F.transform(
                F.split("raw_pgn", "\n"), lambda line: F.regexp_replace(line, r"^\s+|\s+$", "")
            ),
            lambda line: line != "",
        ),
        "\n",
    )
    return records.withColumn(
        "game_id",
        F.when(lichess_id != "", lichess_id).otherwise(
            F.concat(F.lit("sha256-"), F.sha2(canonical, 256))
        ),
    )


def _distinct_records(records: DataFrame) -> DataFrame:
    delivery_order = Window.partitionBy("game_id").orderBy("source_path", "position")
    return (
        records.withColumn("delivery_order", F.row_number().over(delivery_order))
        .where("delivery_order = 1")
        .withColumn("ingested_at", F.current_timestamp())
        .select("game_id", "raw_pgn", *PROVENANCE_COLUMNS, "ingested_at")
    )


def bronze_records(spark: SparkSession, source_path: str) -> DataFrame:
    return _distinct_records(_raw_records(_source_files(spark, source_path)))


def validated_records(bronze: DataFrame) -> DataFrame:
    result = _tag("Result")
    bronze = (
        bronze.withColumn("white_elo_raw", _tag("WhiteElo"))
        .withColumn("black_elo_raw", _tag("BlackElo"))
        .withColumn("time_control_raw", _tag("TimeControl"))
    )
    white_elo = F.expr("try_cast(white_elo_raw AS INT)")
    black_elo = F.expr("try_cast(black_elo_raw AS INT)")
    time_control = F.col("time_control_raw")
    initial = F.expr("try_cast(split(time_control_raw, '[+]')[0] AS INT)")
    increment = F.expr("try_cast(try_element_at(split(time_control_raw, '[+]'), 2) AS INT)")
    valid_time = time_control.rlike(r"^\d+\+\d+$") & initial.isNotNull() & increment.isNotNull()
    played_at = F.try_to_timestamp(
        F.concat_ws(" ", _tag("UTCDate"), _tag("UTCTime")), F.lit("yyyy.MM.dd HH:mm:ss")
    )
    valid_result = result.isin("1-0", "0-1", "1/2-1/2")
    valid_white = F.coalesce(white_elo.between(100, 4000), F.lit(False))
    valid_black = F.coalesce(black_elo.between(100, 4000), F.lit(False))
    has_body = F.exists(
        F.split("raw_pgn", "\n"), lambda line: (F.trim(line) != "") & ~F.trim(line).startswith("[")
    )
    valid = valid_result & valid_white & valid_black & valid_time & played_at.isNotNull() & has_body
    estimated_seconds = initial.cast("long") + increment.cast("long") * F.lit(40)
    average_rating = F.floor((white_elo + black_elo) / F.lit(2)).cast("int")
    bucket_start = F.floor(average_rating / F.lit(200)).cast("int") * F.lit(200)
    errors = F.filter(
        F.array(
            F.when(~valid_result, F.lit("invalid result")),
            F.when(~valid_white, F.lit("invalid white rating")),
            F.when(~valid_black, F.lit("invalid black rating")),
            F.when(~valid_time, F.lit("invalid time control")),
            F.when(played_at.isNull(), F.lit("invalid UTC timestamp")),
            F.when(~has_body, F.lit("missing move text")),
        ),
        lambda item: item.isNotNull(),
    )
    return (
        bronze.withColumn("is_valid", valid)
        .withColumn("validation_errors", errors)
        .withColumn("played_at", played_at)
        .withColumn("white_player", F.when(_tag("White") == "", "unknown").otherwise(_tag("White")))
        .withColumn("black_player", F.when(_tag("Black") == "", "unknown").otherwise(_tag("Black")))
        .withColumn("white_elo", white_elo)
        .withColumn("black_elo", black_elo)
        .withColumn("result", result)
        .withColumn(
            "opening",
            F.when(_tag("Opening") == "", F.lit("Unknown")).otherwise(_tag("Opening")),
        )
        .withColumn("initial_seconds", initial)
        .withColumn("increment_seconds", increment)
        .withColumn(
            "speed",
            F.when(estimated_seconds < 180, "bullet")
            .when(estimated_seconds < 600, "blitz")
            .when(estimated_seconds < 1800, "rapid")
            .otherwise("classical"),
        )
        .withColumn(
            "rating_bucket",
            F.concat(bucket_start.cast("string"), F.lit("-"), (bucket_start + 199).cast("string")),
        )
    )


def _merge_insert_only(spark: SparkSession, frame: DataFrame, table: str) -> None:
    if not spark.catalog.tableExists(table):
        frame.write.format("delta").mode("overwrite").saveAsTable(table)
        return
    # SQL MERGE supports both local Delta tables and Unity Catalog three-part names.
    # The job runs serially; this view is session-scoped and always cleaned up.
    source_view = "chess_merge_source"
    frame.createOrReplaceTempView(source_view)
    try:
        spark.sql(f"""
            MERGE INTO {table} AS target
            USING {source_view} AS source
            ON target.game_id = source.game_id
            WHEN NOT MATCHED THEN INSERT *
        """).collect()
    finally:
        spark.catalog.dropTempView(source_view)


def gold_metrics(silver: DataFrame, dimension: str) -> DataFrame:
    metrics = silver.groupBy(dimension).agg(
        F.count("*").alias("games"),
        F.sum(F.when(F.col("result") == "1-0", 1).otherwise(0)).alias("white_wins"),
        F.sum(F.when(F.col("result") == "1/2-1/2", 1).otherwise(0)).alias("draws"),
    )
    return metrics.withColumn("white_win_rate", F.bround(F.col("white_wins") / F.col("games"), 4))


def main() -> None:
    args = _arguments()
    catalog = _identifier(args.catalog)
    schema = _identifier(args.schema)
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    build_lakehouse(
        spark,
        args.source_path,
        f"{catalog}.{schema}",
        source_url=args.source_url or None,
        requested_max_games=args.requested_max_games,
        retrieved_at_utc=args.retrieved_at_utc or None,
        expected_source_sha256=args.expected_source_sha256 or None,
    )


def build_lakehouse(
    spark: SparkSession,
    source_path: str,
    prefix: str,
    *,
    source_url: str | None = None,
    requested_max_games: int | None = None,
    retrieved_at_utc: str | None = None,
    expected_source_sha256: str | None = None,
) -> dict[str, int]:
    files = _source_files(spark, source_path, source_url, requested_max_games, retrieved_at_utc)
    file_metadata = _file_receipts(files, source_url, requested_max_games, retrieved_at_utc)
    if expected_source_sha256 is not None and (
        len(file_metadata) != 1 or file_metadata[0].source_sha256 != expected_source_sha256
    ):
        raise ValueError("Uploaded PGN does not match the expected receipt SHA-256")
    if any(not row.source_path.lower().endswith(".pgn") for row in file_metadata):
        raise ValueError("Spark expects bounded decompressed .pgn files (not .zst or receipts)")
    raw = _raw_records(files)
    incoming = _distinct_records(raw)
    bronze_table = f"{prefix}.bronze_games"
    existing = spark.table(bronze_table) if spark.catalog.tableExists(bronze_table) else None
    if existing is not None and "batch_id" not in existing.columns:
        raise ValueError("Legacy Bronze IDs/lineage: rebuild into a fresh schema")
    new = (
        incoming
        if existing is None
        else incoming.join(existing.select("game_id"), "game_id", "left_anti")
    )
    # Materialize only per-file summary counts before updating Bronze; no game data collected.
    attempts = {
        row.source_path: row.attempted
        for row in raw.groupBy("source_path").agg(F.count("*").alias("attempted")).collect()
    }
    if requested_max_games is not None and (
        requested_max_games < 1
        or any(attempts.get(row.source_path, 0) != requested_max_games for row in file_metadata)
    ):
        raise ValueError("Requested receipt bound does not match parsed input file(s)")
    if (
        retrieved_at_utc is not None
        and datetime.fromisoformat(retrieved_at_utc).utcoffset() is None
    ):
        raise ValueError("Retrieval timestamp must include a timezone")
    accepted = {
        row.source_path: (row.inserted, row.accepted)
        for row in validated_records(new)
        .groupBy("source_path")
        .agg(
            F.count("*").alias("inserted"), F.sum(F.col("is_valid").cast("long")).alias("accepted")
        )
        .collect()
    }
    _merge_insert_only(spark, incoming, bronze_table)
    bronze = spark.table(f"{prefix}.bronze_games")
    validated = validated_records(bronze)

    silver = validated.where("is_valid").select(
        "game_id",
        "played_at",
        "white_player",
        "black_player",
        "white_elo",
        "black_elo",
        "result",
        "opening",
        "initial_seconds",
        "increment_seconds",
        "speed",
        "rating_bucket",
    )
    quarantine = validated.where("NOT is_valid").select(
        "game_id", "validation_errors", "raw_pgn", *PROVENANCE_COLUMNS, "ingested_at"
    )
    _merge_insert_only(spark, silver, f"{prefix}.silver_games")
    _merge_insert_only(spark, quarantine, f"{prefix}.quarantine_games")

    silver_all = spark.table(f"{prefix}.silver_games")
    for table, dimension in {
        "gold_opening_metrics": "opening",
        "gold_rating_metrics": "rating_bucket",
        "gold_time_control_metrics": "speed",
    }.items():
        metrics = gold_metrics(silver_all, dimension)
        metrics.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(f"{prefix}.{table}")

    counts = {
        "incoming_distinct": incoming.count(),
        "bronze_total": bronze.count(),
        "silver_total": silver_all.count(),
        "quarantine_total": spark.table(f"{prefix}.quarantine_games").count(),
    }
    # One durable history row per input file per successful invocation, including empty files.
    run_id = uuid4().hex
    run_at_utc = datetime.now(UTC).isoformat()
    history = []
    for metadata in file_metadata:
        attempted = attempts.get(metadata.source_path, 0)
        inserted, valid = accepted.get(metadata.source_path, (0, 0))
        history.append(
            (
                *metadata,
                run_id,
                run_at_utc,
                attempted,
                attempted - inserted,
                inserted,
                valid,
                inserted - valid,
                counts["bronze_total"],
                counts["silver_total"],
                counts["quarantine_total"],
            )
        )
    history_schema = StructType(
        [
            *files.select(*PROVENANCE_COLUMNS).schema.fields,
            StructField("run_id", StringType(), False),
            StructField("run_at_utc", StringType(), False),
            *[
                StructField(name, LongType(), False)
                for name in (
                    "attempted",
                    "duplicates",
                    "inserted",
                    "accepted",
                    "quarantined",
                    "bronze_total",
                    "silver_total",
                    "quarantine_total",
                )
            ],
        ]
    )
    spark.createDataFrame(history, history_schema).write.format("delta").mode("append").saveAsTable(
        f"{prefix}.ingestion_runs"
    )
    print(counts)
    return counts


if __name__ == "__main__":
    main()
