import hashlib
import json
import os
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from chess_lakehouse.analytics import ANALYTICS_DIMENSIONS, aggregate_analytics  # noqa: E402
from chess_lakehouse.lineage import batch_id, receipt_path  # noqa: E402
from chess_lakehouse.transform import (  # noqa: E402
    aggregate_gold,
    normalize_game,
    parse_pgn_games,
    stable_game_id,
)
from databricks_job import (  # noqa: E402
    analytics_gold_metrics,
    bronze_records,
    build_lakehouse,
    gold_metrics,
    validated_records,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample.pgn"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("chess-lakehouse-parity-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    if os.environ.get("CHESS_TEST_DELTA") == "1":
        delta = pytest.importorskip("delta")
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.databricks.delta.snapshotPartitions", "2")
        )
        jars = os.environ.get("CHESS_DELTA_JARS")
        if jars:
            builder = builder.config("spark.jars", jars)
        else:
            builder = delta.configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    yield session
    session.stop()


def test_spark_transform_matches_local_ids_validation_and_dimensions(spark):
    bronze = bronze_records(spark, str(FIXTURE))
    rows = {row.game_id: row.asDict() for row in validated_records(bronze).collect()}

    assert set(rows) == {"alpha001", "bad00001"}
    assert rows["alpha001"]["is_valid"] is True
    assert rows["alpha001"]["opening"] == "Sicilian Defense"
    assert rows["alpha001"]["rating_bucket"] == "1600-1799"
    assert rows["alpha001"]["speed"] == "blitz"
    assert rows["alpha001"]["eco"] == "Unknown"
    assert rows["alpha001"]["plies"] == 4
    assert rows["alpha001"]["rating_gap"] == -200
    assert rows["bad00001"]["is_valid"] is False
    assert rows["bad00001"]["validation_errors"] == [
        "invalid result",
        "invalid white rating",
        "invalid time control",
    ]


def test_spark_gold_has_same_grain_and_metrics_as_local_path(spark):
    silver = validated_records(bronze_records(spark, str(FIXTURE))).where("is_valid")

    rows = [row.asDict() for row in gold_metrics(silver, "opening").collect()]

    assert rows == [
        {
            "opening": "Sicilian Defense",
            "games": 1,
            "white_wins": 1,
            "draws": 0,
            "white_win_rate": 1.0,
        }
    ]


def test_missing_fields_and_non_lichess_fallback_match_local(spark, tmp_path):
    raw = next(iter(parse_pgn_games(FIXTURE.read_text())))
    bad = (
        raw.replace("https://lichess.org/alpha001", "https://example.org/lichess.org/alpha001")
        .replace('[WhiteElo "1500"]\n', "")
        .replace("2026.01.02", "2026.02.30")
    )
    path = tmp_path / "missing.pgn"
    path.write_text(bad + "\n\n")
    rows = validated_records(bronze_records(spark, str(path))).collect()
    assert len(rows) == 1
    assert rows[0].game_id == stable_game_id(bad)
    assert normalize_game(bad)[0] is None
    assert rows[0].is_valid is False
    assert rows[0].validation_errors == ["invalid white rating", "invalid UTC timestamp"]


@pytest.mark.skipif(
    not os.environ.get("CHESS_REAL_SAMPLE"), reason="set CHESS_REAL_SAMPLE for real-data parity"
)
def test_real_sample_matches_local_silver_and_all_gold(spark):
    path = Path(os.environ["CHESS_REAL_SAMPLE"])
    local = {}
    for raw in parse_pgn_games(path.read_text()):
        game, _ = normalize_game(raw)
        if game is not None:
            local.setdefault(game["game_id"], game)
    silver = validated_records(bronze_records(spark, str(path))).where("is_valid")
    fields = list(next(iter(local.values())))
    actual = silver.withColumn("played_at", F.date_format("played_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    assert {row.game_id: row.asDict() for row in actual.select(*fields).collect()} == local
    for table, expected in aggregate_gold(local.values()).items():
        dimension = {
            "opening_metrics": "opening",
            "rating_metrics": "rating_bucket",
            "time_control_metrics": "speed",
        }[table]
        actual_gold = [
            row.asDict() for row in gold_metrics(silver, dimension).orderBy(dimension).collect()
        ]
        assert actual_gold == expected
        print(f"{table}: {len(actual_gold)} rows; {sum(row['games'] for row in actual_gold)} games")
    for table, expected in aggregate_analytics(local.values()).items():
        actual_gold = [
            row.asDict()
            for row in analytics_gold_metrics(silver, table)
            .orderBy(*ANALYTICS_DIMENSIONS[table])
            .collect()
        ]
        assert actual_gold == expected


@pytest.mark.skipif(
    os.environ.get("CHESS_TEST_DELTA") != "1", reason="set CHESS_TEST_DELTA=1 for Delta integration"
)
def test_delta_managed_tables_replay(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS chess_test")
    expected = {"incoming_distinct": 2, "bronze_total": 2, "silver_total": 1, "quarantine_total": 1}
    assert build_lakehouse(spark, str(FIXTURE), "spark_catalog.chess_test") == expected
    assert build_lakehouse(spark, str(FIXTURE), "spark_catalog.chess_test") == expected
    for table in ("bronze_games", "silver_games", "quarantine_games"):
        frame = spark.table(f"chess_test.{table}")
        assert frame.count() == frame.select("game_id").distinct().count()
    for table in ("opening_metrics", "rating_metrics", "time_control_metrics"):
        rows = spark.table(f"chess_test.gold_{table}").collect()
        assert len(rows) == 1
        assert rows[0].games == rows[0].white_wins == 1
        assert rows[0].draws == 0


@pytest.mark.skipif(
    os.environ.get("CHESS_TEST_DELTA") != "1" or not os.environ.get("CHESS_REAL_SAMPLE"),
    reason="set CHESS_TEST_DELTA=1 and CHESS_REAL_SAMPLE for real Delta replay",
)
def test_real_delta_managed_tables_replay_matches_local(spark):
    path = Path(os.environ["CHESS_REAL_SAMPLE"])
    distinct = {}
    for raw in parse_pgn_games(path.read_text()):
        distinct.setdefault(stable_game_id(raw), normalize_game(raw)[0])
    local = [game for game in distinct.values() if game is not None]
    expected = {
        "incoming_distinct": len(distinct),
        "bronze_total": len(distinct),
        "silver_total": len(local),
        "quarantine_total": len(distinct) - len(local),
    }
    spark.sql("CREATE SCHEMA IF NOT EXISTS chess_real_test")
    receipt = json.loads(receipt_path(path).read_text()) if receipt_path(path).exists() else {}
    source_url = receipt.get(
        "source_url",
        "https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst",
    )
    for _ in range(2):
        assert (
            build_lakehouse(
                spark,
                str(path),
                "spark_catalog.chess_real_test",
                source_url=source_url,
                requested_max_games=receipt.get("requested_max_games"),
                retrieved_at_utc=receipt.get("retrieved_at_utc"),
                expected_source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
            == expected
        )
    bronze = spark.table("chess_real_test.bronze_games")
    assert all(len(row.game_id) == 8 for row in bronze.select("game_id").collect())
    expected_batch = batch_id(source_url, hashlib.sha256(path.read_bytes()).hexdigest())
    assert {row.batch_id for row in bronze.select("batch_id").distinct().collect()} == {
        expected_batch
    }
    history = spark.table("chess_real_test.ingestion_runs").collect()
    assert len(history) == 2 and all(
        row.source_kind == "lichess_public" and row.source_license == "CC0-1.0" for row in history
    )
    assert all(
        row.requested_max_games == receipt.get("requested_max_games")
        and row.retrieved_at_utc == receipt.get("retrieved_at_utc")
        for row in history
    )
    assert {row.batch_id for row in history} == {expected_batch}
    assert sorted(row.inserted for row in history) == [0, len(distinct)]
    for table in ("bronze_games", "silver_games", "quarantine_games"):
        frame = spark.table(f"chess_real_test.{table}")
        assert frame.count() == frame.select("game_id").distinct().count()
    for table, rows in aggregate_gold(local).items():
        dimension = {
            "opening_metrics": "opening",
            "rating_metrics": "rating_bucket",
            "time_control_metrics": "speed",
        }[table]
        actual = spark.table(f"chess_real_test.gold_{table}").orderBy(dimension).collect()
        assert [row.asDict() for row in actual] == rows
    for table, rows in aggregate_analytics(local).items():
        actual = (
            spark.table(f"chess_real_test.gold_{table}")
            .orderBy(*ANALYTICS_DIMENSIONS[table])
            .collect()
        )
        assert [row.asDict() for row in actual] == rows


def test_spark_logical_id_and_header_only_boundary(spark, tmp_path):
    base = next(iter(parse_pgn_games(FIXTURE.read_text())))
    bad = base.split("\n\n")[0].replace('[WhiteElo "1500"]\n', "")
    raw = (
        base.replace("alpha001", "qa000001")
        .replace("lichess.org/", "lichess.org/game/")
        .replace('[Opening "Sicilian Defense"]', '[ExtraTag "ignored"]')
    )
    path = tmp_path / "batch.pgn"
    path.write_text(bad + "\n\n" + raw + "\n\n" + raw.replace('Event "', 'Event "Replay '))
    rows = {r.game_id: r for r in validated_records(bronze_records(spark, str(path))).collect()}
    assert set(rows) == {"alpha001", "qa000001"}
    assert not rows["alpha001"].is_valid
    assert "missing move text" in rows["alpha001"].validation_errors
    assert rows["qa000001"].is_valid and rows["qa000001"].opening == "Unknown"


@pytest.mark.skipif(
    os.environ.get("CHESS_TEST_DELTA") != "1", reason="set CHESS_TEST_DELTA=1 for Delta integration"
)
def test_delta_logical_replay_and_lineage(spark, tmp_path):
    spark.sql("CREATE SCHEMA IF NOT EXISTS chess_lineage_test")
    prefix = "spark_catalog.chess_lineage_test"
    base = next(iter(parse_pgn_games(FIXTURE.read_text())))
    raw = base.replace("lichess.org/alpha001", "lichess.org/game/alpha001")
    path = tmp_path / "batch.pgn"
    path.write_text(raw)
    source_url = "fixture://logical"
    expected = {"incoming_distinct": 1, "bronze_total": 1, "silver_total": 1, "quarantine_total": 0}
    assert build_lakehouse(spark, str(path), prefix, source_url=source_url) == expected
    path.write_text(raw.replace('Event "', 'Event "Replay '))
    assert build_lakehouse(spark, str(path), prefix, source_url=source_url) == expected
    path.write_text("")
    assert build_lakehouse(spark, str(path), prefix, source_url=source_url) == {
        **expected,
        "incoming_distinct": 0,
    }
    history = spark.table(f"{prefix}.ingestion_runs").collect()
    assert len(history) == 3 and len({row.run_id for row in history}) == 3
    assert len({row.batch_id for row in history}) == 3
    assert sorted(
        (row.attempted, row.duplicates, row.inserted, row.accepted, row.quarantined)
        for row in history
    ) == [(0, 0, 0, 0, 0), (1, 0, 1, 1, 0), (1, 1, 0, 0, 0)]
    bronze = spark.table(f"{prefix}.bronze_games").first()
    assert bronze.game_id == "alpha001" and bronze.source_url == source_url
    assert bronze.source_kind == "synthetic_fixture" and bronze.source_license == "MIT"
    assert bronze.pipeline_version and bronze.batch_id in {row.batch_id for row in history}
    assert all(row.bronze_total == row.silver_total == 1 for row in history)


@pytest.mark.skipif(os.environ.get("CHESS_TEST_DELTA") != "1", reason="requires local Delta")
def test_delta_multi_file_glob_with_receipts_and_legacy_guard(spark, tmp_path):
    prefix = "spark_catalog.chess_chunks_test"
    spark.sql("CREATE SCHEMA IF NOT EXISTS chess_chunks_test")
    records = list(parse_pgn_games(FIXTURE.read_text()))
    first = tmp_path / "part-01.pgn"
    second = tmp_path / "part-02.pgn"
    first.write_text(records[0] + "\n\n" + records[2] + "\n")
    second.write_text(records[1] + "\n\n" + records[0].replace("alpha001", "new00001") + "\n")
    hashes = {hashlib.sha256(p.read_bytes()).hexdigest() for p in (first, second)}
    for _ in range(2):
        counts = build_lakehouse(
            spark,
            str(tmp_path / "*.pgn"),
            prefix,
            source_url="fixture://chunks",
            requested_max_games=2,
        )
        assert counts == {
            "incoming_distinct": 3,
            "bronze_total": 3,
            "silver_total": 2,
            "quarantine_total": 1,
        }
    history = spark.table(f"{prefix}.ingestion_runs").collect()
    assert len(history) == 4 and {r.source_sha256 for r in history} == hashes
    assert all(r.attempted == 2 and r.requested_max_games == 2 for r in history)
    by_run = {}
    for row in history:
        by_run[row.run_id] = by_run.get(row.run_id, 0) + row.inserted
    assert sorted(by_run.values()) == [0, 3]
    for table in ANALYTICS_DIMENSIONS:
        assert spark.table(f"{prefix}.gold_{table}").agg(F.sum("games")).first()[0] == 2
    spark.sql("CREATE SCHEMA IF NOT EXISTS chess_legacy_test")
    spark.createDataFrame([("old",)], "game_id string").write.format("delta").saveAsTable(
        "chess_legacy_test.silver_games"
    )
    with pytest.raises(ValueError, match="fresh schema"):
        build_lakehouse(spark, str(first), "spark_catalog.chess_legacy_test")
    assert not spark.catalog.tableExists("chess_legacy_test.bronze_games")


def test_spark_half_even_and_optional_fields(spark, tmp_path):
    base = next(iter(parse_pgn_games(FIXTURE.read_text())))
    annotated = base.replace(
        '[Opening "Sicilian Defense"]',
        '[Opening "Sicilian Defense"]\n[ECO "B20"]\n[Termination "Time forfeit"]',
    ).replace("1. e4 c5 2. Nf3 d6 1-0", "1. e4 { [%clk 0:05:00] } 1... c5 2. Nf3 d6 1-0")
    unknown = (
        annotated.replace("alpha001", "beta0002")
        .replace("B20", "?")
        .replace("Time forfeit", "unsupported")
        .replace("1. e4 { [%clk 0:05:00] } 1... c5 2. Nf3 d6 1-0", "1-0")
    )
    path = tmp_path / "optional.pgn"
    path.write_text(annotated + "\n\n" + unknown + "\n")
    expected = {
        game["game_id"]: game
        for raw in parse_pgn_games(path.read_text())
        if (game := normalize_game(raw)[0]) is not None
    }
    actual = (
        validated_records(bronze_records(spark, str(path)))
        .where("is_valid")
        .withColumn("played_at", F.date_format("played_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )
    fields = list(next(iter(expected.values())))
    assert {row.game_id: row.asDict() for row in actual.select(*fields).collect()} == expected
    assert expected["beta0002"]["plies"] is None and expected["beta0002"]["eco"] == "Unknown"
    frame = spark.range(160).select(
        F.lit("Italian Game").alias("opening"),
        F.lit("C50").alias("eco"),
        F.lit("1600-1799").alias("rating_bucket"),
        F.lit("blitz").alias("speed"),
        F.when(F.col("id") < 77, "1-0").otherwise("0-1").alias("result"),
    )
    for row in (
        gold_metrics(frame, "opening").first(),
        analytics_gold_metrics(frame, "opening_context").first(),
    ):
        assert row.games == 160 and row.white_wins == 77 and row.white_win_rate == 0.4812
