"""Verify a bounded multi-file sample with local Spark and independent Python aggregation."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import tempfile
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit

from chess_lakehouse import __version__
from chess_lakehouse.analytics import ANALYTICS_DIMENSIONS
from chess_lakehouse.lineage import input_receipt, receipt_path, source_metadata
from chess_lakehouse.transform import normalize_game, stable_game_id, stream_pgn_games
from scripts.export_dashboard import DIMENSIONS, export_verified_snapshot

MIN_FREE_BYTES = 15 * 1024**3


def check_disk(path: Path) -> None:
    if shutil.disk_usage(path).free < MIN_FREE_BYTES:
        raise RuntimeError("Less than 15 GiB free disk; stop and review available storage")


def verify_chunks(directory: Path, games: int) -> tuple[dict, list[dict]]:
    manifest = json.loads((directory / "manifest.json").read_text())
    bound = manifest["games_per_file"]
    if bound <= 0 or games <= 0 or games % bound or games > manifest["total_games"]:
        raise ValueError("Invalid checkpoint: use a positive whole-file prefix")
    if manifest["total_games"] != bound * len(manifest["parts"]):
        raise ValueError("Chunk manifest total does not match its file count")
    expected_files = [f"part-{number:03d}.pgn" for number in range(1, len(manifest["parts"]) + 1)]
    if [part["file"] for part in manifest["parts"]] != expected_files or sorted(
        path.name for path in directory.glob("part-*.pgn")
    ) != sorted(expected_files):
        raise ValueError("Chunk file list does not match manifest")
    parts = manifest["parts"][: games // bound]
    prefix_hash = hashlib.sha256()
    for index, part in enumerate(parts, 1):
        expected_name = f"part-{index:03d}.pgn"
        if (
            part["file"] != expected_name
            or part["games"] != bound
            or part["first_record"] != (index - 1) * bound + 1
            or part["last_record"] != index * bound
        ):
            raise ValueError("Invalid chunk manifest range or file order")
        path = directory / expected_name
        checksum = hashlib.sha256()
        if index > 1:
            prefix_hash.update(b"\n")
        size = 0
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                checksum.update(block)
                prefix_hash.update(block)
                size += len(block)
        if checksum.hexdigest() != part["source_sha256"] or size != part["bytes"]:
            raise ValueError(f"Chunk SHA-256 or size mismatch: {expected_name}")
        provenance = input_receipt(path, manifest["source_url"], checksum.hexdigest())
        receipt = json.loads(receipt_path(path).read_text())
        if (
            provenance["requested_max_games"] != bound
            or provenance["retrieved_at_utc"] != manifest["retrieved_at_utc"]
            or receipt["bytes_written"] != size
            or receipt["parent_source_sha256"] != manifest["parent_source_sha256"]
            or receipt["parent_requested_max_games"] != manifest["total_games"]
            or receipt["chunk_number"] != index
        ):
            raise ValueError(f"Invalid chunk receipt: {expected_name}")
    if (
        games == manifest["total_games"]
        and prefix_hash.hexdigest() != manifest["parent_source_sha256"]
    ):
        raise ValueError("Reconstructed chunk SHA-256 does not match parent")
    return {
        **source_metadata(manifest["source_url"]),
        "source_url": manifest["source_url"],
        "source_sha256": prefix_hash.hexdigest(),
        "requested_max_games": games,
        "retrieved_at_utc": manifest["retrieved_at_utc"],
        "pipeline_version": __version__,
    }, parts


def independent_silver(directory: Path, parts: list[dict], counts: dict):
    """Keep only unique IDs and yield accepted records one at a time."""
    ids = set()
    rejected = 0
    for part in parts:
        attempted = 0
        with (directory / part["file"]).open(encoding="utf-8") as handle:
            for raw in stream_pgn_games(handle):
                attempted += 1
                game_id = stable_game_id(raw)
                if game_id in ids:
                    continue
                ids.add(game_id)
                normalized, _ = normalize_game(raw)
                if normalized is None:
                    rejected += 1
                else:
                    yield normalized
        if attempted != part["games"]:
            raise ValueError(f"Python record count mismatch: {part['file']}")
    if len(ids) != counts["bronze"] or rejected != counts["quarantine"]:
        raise ValueError("Python distinct or quarantine totals differ from Spark")


def write_gold(frame, destination: Path, dimensions: tuple[str, ...], expected: int) -> int:
    rows = 0
    total = 0
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=frame.columns)
        writer.writeheader()
        for row in frame.orderBy(*dimensions).toLocalIterator():
            writer.writerow(row.asDict())
            rows += 1
            total += row.games
    if total != expected:
        raise ValueError(f"Gold total mismatch: {destination.name}")
    return rows


def spark_gold(spark, directory: Path, parts: list[dict], gold_dir: Path) -> tuple[dict, dict]:
    from pyspark import StorageLevel

    from databricks_job import (
        _distinct_records,
        _file_receipts,
        _raw_records,
        _source_files,
        analytics_gold_metrics,
        gold_metrics,
        validated_records,
    )

    paths = [str((directory / part["file"]).resolve()) for part in parts]
    # Full samples use one glob; checkpoints use an explicit prefix of the verified files.
    source = (
        str(directory.resolve() / "part-*.pgn")
        if len(paths) == len(list(directory.glob("part-*.pgn")))
        else paths
    )
    files = _source_files(spark, source)
    expected = {part["file"]: part for part in parts}
    receipts = _file_receipts(files, None, None, None)
    hashes = {
        Path(unquote(urlsplit(row.source_path).path)).name: row.source_sha256 for row in receipts
    }
    if hashes != {name: part["source_sha256"] for name, part in expected.items()}:
        raise ValueError("Spark source hashes do not match chunk manifest")
    raw = _raw_records(files)
    attempts = {
        Path(unquote(urlsplit(row.source_path).path)).name: row["count"]
        for row in raw.groupBy("source_path").count().collect()
    }
    if attempts != {name: part["games"] for name, part in expected.items()}:
        raise ValueError("Spark attempted counts do not match chunk manifest")
    print(f"Verified {len(parts)} files and {sum(attempts.values()):,} attempted games", flush=True)
    check_disk(gold_dir)
    # Cache the compact projection on disk, never raw PGN or player data.
    fields = sorted({dim for dims in ANALYTICS_DIMENSIONS.values() for dim in dims})
    validated = (
        validated_records(_distinct_records(raw))
        .select("is_valid", "result", "plies", *fields)
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        by_valid = {
            row.is_valid: row["count"] for row in validated.groupBy("is_valid").count().collect()
        }
        if None in by_valid:
            raise ValueError("Unexpected null validation status")
        counts = {
            "bronze": sum(by_valid.values()),
            "silver": by_valid.get(True, 0),
            "quarantine": by_valid.get(False, 0),
        }
        print(f"Spark distinct counts: {counts}", flush=True)
        check_disk(gold_dir)
        silver = validated.where("is_valid")
        groups = {}
        for dimension, filename in DIMENSIONS.items():
            groups[filename.removesuffix(".csv")] = write_gold(
                gold_metrics(silver, dimension), gold_dir / filename, (dimension,), counts["silver"]
            )
        for table, dimensions in ANALYTICS_DIMENSIONS.items():
            check_disk(gold_dir)
            groups[table] = write_gold(
                analytics_gold_metrics(silver, table),
                gold_dir / f"{table}.csv",
                dimensions,
                counts["silver"],
            )
        return counts, groups
    finally:
        validated.unpersist()


def run_checkpoint(directory: Path, games: int, output: Path, master: str, memory: str) -> dict:
    from pyspark.sql import SparkSession

    if output.exists():
        raise ValueError("Choose a new checkpoint output directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    check_disk(output.parent)
    started = time.monotonic()
    metadata, parts = verify_chunks(directory, games)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".scale-") as temporary:
        stage = Path(temporary) / "results"
        gold_dir = stage / "gold"
        gold_dir.mkdir(parents=True)
        scratch = Path(temporary) / "spark"
        scratch.mkdir()
        spark = (
            SparkSession.builder.master(master)
            .appName(f"chess-lakehouse-{games}")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", memory)
            .config("spark.local.dir", str(scratch.resolve()))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.files.maxPartitionBytes", str(32 * 1024**2))
            .config("spark.sql.shuffle.partitions", "64")
            .getOrCreate()
        )
        try:
            spark.sparkContext.setLogLevel("WARN")
            counts, groups = spark_gold(spark, directory, parts, gold_dir)
        finally:
            spark.stop()
        spark_seconds = time.monotonic() - started
        print(
            "Spark Gold complete; independently reconciling against streamed Python PGN", flush=True
        )
        check_disk(gold_dir)
        metadata["cumulative_counts"] = dict(sorted(counts.items()))
        snapshot = export_verified_snapshot(
            metadata,
            independent_silver(directory, parts, counts),
            gold_dir,
            stage / "snapshot.json",
        )
        with (stage / "snapshot.json").open("rb") as handle:
            snapshot_hash = hashlib.file_digest(handle, "sha256").hexdigest()
        report = {
            "source": snapshot["source"],
            "counts": counts,
            "outcomes": snapshot["outcomes"],
            "coverage": snapshot["coverage"],
            "gold_groups": groups,
            "verified_files": len(parts),
            "snapshot_sha256": snapshot_hash,
            "spark_and_receipt_seconds": round(spark_seconds, 2),
            "total_seconds": round(time.monotonic() - started, 2),
        }
        (stage / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        stage.rename(output)
        return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks", type=Path, required=True)
    parser.add_argument("--games", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="4g")
    args = parser.parse_args()
    print(
        json.dumps(
            run_checkpoint(args.chunks, args.games, args.output, args.master, args.driver_memory),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
