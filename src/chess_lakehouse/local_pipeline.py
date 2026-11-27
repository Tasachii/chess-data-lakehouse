from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from chess_lakehouse import __version__
from chess_lakehouse.compression import decompressed_chunks
from chess_lakehouse.lineage import input_receipt
from chess_lakehouse.transform import (
    aggregate_gold,
    normalize_game,
    parse_pgn_games,
    stable_game_id,
)


def _read_source(path: Path, source_bytes: bytes) -> str:
    if path.suffix == ".zst":
        return b"".join(decompressed_chunks(io.BytesIO(source_bytes))).decode("utf-8")
    return source_bytes.decode("utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    path.write_text(content)


def _write_csv(path: Path, rows: list[dict[str, Any]], dimension: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[dimension, "games", "white_wins", "draws", "white_win_rate"],
        )
        writer.writeheader()
        writer.writerows(rows)


def run_pipeline(input_path: str | Path, output_dir: str | Path, source_url: str) -> dict[str, int]:
    source = Path(input_path)
    output = Path(output_dir)
    source_bytes = source.read_bytes()
    blocks = list(parse_pgn_games(_read_source(source, source_bytes)))
    provenance = input_receipt(source, source_url, hashlib.sha256(source_bytes).hexdigest())

    if provenance["requested_max_games"] is not None and provenance["requested_max_games"] != len(
        blocks
    ):
        raise ValueError("Download receipt record bound does not match parsed input")

    bronze_path = output / "bronze" / "games.jsonl"
    bronze = _read_jsonl(bronze_path)
    if any("batch_id" not in row for row in bronze):
        raise ValueError("Legacy Bronze IDs/lineage: rebuild into a fresh output directory")
    known_ids = {row["game_id"] for row in bronze}
    inserted_ids: set[str] = set()
    for raw_pgn in blocks:
        game_id = stable_game_id(raw_pgn)
        if game_id not in known_ids:
            bronze.append(
                {
                    "game_id": game_id,
                    "raw_pgn": raw_pgn,
                    "source_url": source_url,
                    "source_sha256": provenance["source_sha256"],
                    "batch_id": provenance["batch_id"],
                }
            )
            known_ids.add(game_id)
            inserted_ids.add(game_id)
    bronze.sort(key=lambda row: row["game_id"])
    _write_jsonl(bronze_path, bronze)

    silver: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    for row in bronze:
        normalized, errors = normalize_game(row["raw_pgn"])
        if normalized is None:
            quarantine.append(
                {"game_id": row["game_id"], "errors": errors, "raw_pgn": row["raw_pgn"]}
            )
        else:
            silver.append(normalized)
    _write_jsonl(output / "silver" / "games.jsonl", silver)
    _write_jsonl(output / "quarantine" / "games.jsonl", quarantine)

    gold = aggregate_gold(silver)
    _write_csv(output / "gold" / "opening_metrics.csv", gold["opening_metrics"], "opening")
    _write_csv(output / "gold" / "rating_metrics.csv", gold["rating_metrics"], "rating_bucket")
    _write_csv(output / "gold" / "time_control_metrics.csv", gold["time_control_metrics"], "speed")

    counts = {
        "input_games": len(blocks),
        "inserted_bronze": len(inserted_ids),
        "valid_silver": len(silver),
        "quarantined": len(quarantine),
    }
    accepted = sum(row["game_id"] in inserted_ids for row in silver)
    run_id = uuid4().hex
    manifest = {
        **provenance,
        "run_id": run_id,
        "run_at_utc": datetime.now(UTC).isoformat(),
        "pipeline_version": __version__,
        "source_file": source.name,
        "counts": counts,
        "batch_counts": {
            "attempted": len(blocks),
            "duplicates": len(blocks) - len(inserted_ids),
            "inserted": len(inserted_ids),
            "accepted": accepted,
            "quarantined": len(inserted_ids) - accepted,
        },
        "cumulative_counts": {
            "bronze": len(bronze),
            "silver": len(silver),
            "quarantine": len(quarantine),
        },
    }
    manifest_dir = output / "manifests"
    history = manifest_dir / "runs" / f"{run_id}.json"
    history.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    history.write_text(content, encoding="utf-8")
    (manifest_dir / "latest.json").write_text(content, encoding="utf-8")
    return counts
