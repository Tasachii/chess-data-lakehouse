import hashlib
import json
from pathlib import Path

import pytest
import zstandard

from chess_lakehouse.source import download_bounded_sample
from scripts.scale_sample import verify_chunks
from scripts.split_sample import split_sample

FIXTURE = Path(__file__).parent / "fixtures" / "sample.pgn"


def test_checkpoint_hash_and_chunk_receipts(tmp_path):
    packed = tmp_path / "source.zst"
    packed.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes()))
    sample = tmp_path / "sample.pgn"
    download_bounded_sample(packed.as_uri(), sample, 3)
    chunks = tmp_path / "chunks"
    manifest = split_sample(sample, chunks, 1)
    metadata, parts = verify_chunks(chunks, 2)
    parent = b"\n".join((chunks / part["file"]).read_bytes() for part in parts)
    assert metadata["source_sha256"] == hashlib.sha256(parent).hexdigest()
    assert metadata["requested_max_games"] == 2
    full, _ = verify_chunks(chunks, 3)
    assert full["source_sha256"] == manifest["parent_source_sha256"]
    with pytest.raises(ValueError, match="checkpoint"):
        verify_chunks(chunks, 4)
    extra = chunks / "part-999.pgn"
    extra.write_bytes(b"stale")
    with pytest.raises(ValueError, match="file list"):
        verify_chunks(chunks, 3)
    extra.unlink()
    file = chunks / parts[0]["file"]
    file.write_bytes(file.read_bytes().replace(b"Event", b"event", 1))
    with pytest.raises(ValueError, match="SHA-256"):
        verify_chunks(chunks, 2)


def test_checkpoint_rejects_tampered_per_file_receipt(tmp_path):
    packed = tmp_path / "source.zst"
    packed.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes()))
    sample = tmp_path / "sample.pgn"
    download_bounded_sample(packed.as_uri(), sample, 3)
    chunks = tmp_path / "chunks"
    split_sample(sample, chunks, 1)
    receipt = chunks / "part-001.pgn.receipt.json"
    data = json.loads(receipt.read_text())
    data["requested_max_games"] = 2
    receipt.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="receipt"):
        verify_chunks(chunks, 2)


def test_streamed_export_rejects_incorrect_half_tie(tmp_path):
    import csv

    from chess_lakehouse.analytics import ANALYTICS_DIMENSIONS, aggregate_analytics
    from chess_lakehouse.transform import aggregate_gold, normalize_game, parse_pgn_games
    from scripts.export_dashboard import DIMENSIONS, export_verified_snapshot

    game, _ = normalize_game(next(iter(parse_pgn_games(FIXTURE.read_text()))))
    games = [dict(game, result="1-0" if i < 77 else "0-1") for i in range(160)]
    groups = {**aggregate_gold(games), **aggregate_analytics(games)}
    gold = tmp_path / "gold"
    gold.mkdir()
    for table, rows in groups.items():
        if table == "opening_metrics":
            rows[0]["white_win_rate"] = 0.4813
        with (gold / f"{table}.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    assert set(groups) == {name.removesuffix(".csv") for name in DIMENSIONS.values()} | set(
        ANALYTICS_DIMENSIONS
    )
    manifest = {
        "source_url": "https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst",
        "source_kind": "lichess_public",
        "source_license": "CC0-1.0",
        "source_license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
        "source_sha256": "0" * 64,
        "retrieved_at_utc": "2026-11-27T00:00:00+00:00",
        "requested_max_games": 160,
        "pipeline_version": "0.3.0",
        "cumulative_counts": {"bronze": 160, "silver": 160, "quarantine": 0},
    }
    destination = tmp_path / "snapshot.json"
    with pytest.raises(ValueError, match="Gold rate mismatch"):
        export_verified_snapshot(manifest, iter(games), gold, destination)
    assert not destination.exists()
