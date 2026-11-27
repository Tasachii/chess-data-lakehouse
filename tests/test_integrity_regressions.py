"""Test game IDs, record boundaries, truncated input and source lineage."""

import hashlib
import json
from pathlib import Path

import pytest
import zstandard

from chess_lakehouse.local_pipeline import run_pipeline
from chess_lakehouse.source import _stream_pgn_games, download_bounded_sample
from chess_lakehouse.transform import parse_pgn_games, stable_game_id

FIXTURE = Path(__file__).parent / "fixtures" / "sample.pgn"


def _base():
    return next(iter(parse_pgn_games(FIXTURE.read_text())))


def _read(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_logical_lichess_replay(tmp_path):
    raw = _base().replace("lichess.org/alpha001", "lichess.org/game/alpha001")
    assert stable_game_id(raw) == "alpha001"
    assert stable_game_id(raw.replace("/game/", "/")) == "alpha001"
    assert stable_game_id(raw.replace("lichess.org", "example.org/lichess.org")).startswith(
        "sha256-"
    )
    path = tmp_path / "batch.pgn"
    out = tmp_path / "out"
    path.write_text(raw)
    assert run_pipeline(path, out, "fixture://logical")["inserted_bronze"] == 1
    path.write_text(raw.replace('Event "', 'Event "Replay '))
    assert run_pipeline(path, out, "fixture://logical")["inserted_bronze"] == 0
    assert len(_read(out / "silver/games.jsonl")) == 1


def test_header_only_record_cannot_contaminate_next_record(tmp_path):
    base = _base()
    bad = base.split("\n\n")[0].replace('[WhiteElo "1500"]\n', "")
    new = base.replace("alpha001", "qa000001").replace(
        '[Opening "Sicilian Defense"]', '[ExtraTag "ignored"]'
    )
    combined = bad + "\n\n" + new
    assert (
        list(parse_pgn_games(combined))
        == list(_stream_pgn_games(combined.splitlines()))
        == [bad, new]
    )
    path = tmp_path / "batch.pgn"
    path.write_text(combined)
    out = tmp_path / "out"
    assert run_pipeline(path, out, "fixture://boundary") == {
        "input_games": 2,
        "inserted_bronze": 2,
        "valid_silver": 1,
        "quarantined": 1,
    }
    assert _read(out / "silver/games.jsonl")[0]["opening"] == "Unknown"
    assert _read(out / "quarantine/games.jsonl")[0]["game_id"] == "alpha001"


@pytest.mark.parametrize("cut", [1, 10, 30])
def test_truncated_zstd_never_publishes(tmp_path, cut):
    compressed = tmp_path / "bad.zst"
    compressed.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes())[:-cut])
    out = tmp_path / "out"
    with pytest.raises((ValueError, zstandard.ZstdError)):
        run_pipeline(compressed, out, "fixture://truncated")
    assert not out.exists()
    run_pipeline(FIXTURE, out, "fixture://baseline")
    before = {f.relative_to(out): f.read_bytes() for f in out.rglob("*") if f.is_file()}
    with pytest.raises((ValueError, zstandard.ZstdError)):
        run_pipeline(compressed, out, "fixture://truncated")
    assert before == {f.relative_to(out): f.read_bytes() for f in out.rglob("*") if f.is_file()}


def test_download_rejects_early_eof_and_short_complete_source(tmp_path):
    source = tmp_path / "source.zst"
    content = zstandard.ZstdCompressor().compress(FIXTURE.read_bytes())
    destination = tmp_path / "sample.pgn"
    for data in (content[:-1], content):
        source.write_bytes(data)
        with pytest.raises((ValueError, zstandard.ZstdError)):
            download_bounded_sample(source.as_uri(), destination, max_games=4)
        assert not destination.exists()


def test_durable_batch_lineage_and_reconciliation(tmp_path):
    out = tmp_path / "out"
    run_pipeline(FIXTURE, out, "fixture://initial")
    first = json.loads((out / "manifests/latest.json").read_text())
    run_pipeline(FIXTURE, out, "fixture://initial")
    replay = json.loads((out / "manifests/latest.json").read_text())
    overlap = tmp_path / "overlap.pgn"
    overlap.write_text(_base() + "\n\n" + _base().replace("alpha001", "qa000001"))
    run_pipeline(overlap, out, "fixture://overlap")
    empty = tmp_path / "empty.pgn"
    empty.write_text("")
    run_pipeline(empty, out, "fixture://empty")
    history = [json.loads(f.read_text()) for f in (out / "manifests/runs").glob("*.json")]
    assert len(history) == 4 and len({row["run_id"] for row in history}) == 4
    assert first["batch_id"] == replay["batch_id"]
    assert first["source_kind"] == "synthetic_fixture" and first["source_license"] == "MIT"
    assert first["pipeline_version"] and first["retrieved_at_utc"] is None
    assert first["requested_max_games"] is None
    assert first["batch_counts"] == {
        "attempted": 3,
        "duplicates": 1,
        "inserted": 2,
        "accepted": 1,
        "quarantined": 1,
    }
    assert replay["batch_counts"] == {
        "attempted": 3,
        "duplicates": 3,
        "inserted": 0,
        "accepted": 0,
        "quarantined": 0,
    }
    latest = json.loads((out / "manifests/latest.json").read_text())
    assert latest["batch_counts"] == dict.fromkeys(first["batch_counts"], 0)
    assert latest["cumulative_counts"] == {"bronze": 3, "silver": 2, "quarantine": 1}
    by_batch = {row["batch_id"]: row for row in history}
    for row in _read(out / "bronze/games.jsonl"):
        assert row["source_sha256"] == by_batch[row["batch_id"]]["source_sha256"]
    assert first["source_sha256"] == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()


def test_download_receipt_propagates_retrieval_bound(tmp_path):
    source = tmp_path / "source.zst"
    source.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes()))
    sample = tmp_path / "sample.pgn"
    download_bounded_sample(source.as_uri(), sample, 2)
    run_pipeline(sample, tmp_path / "out", source.as_uri())
    manifest = json.loads((tmp_path / "out/manifests/latest.json").read_text())
    assert manifest["requested_max_games"] == 2
    assert manifest["retrieved_at_utc"] and manifest["retrieved_at_utc"] <= manifest["run_at_utc"]


def test_header_only_with_all_tags_is_quarantined(tmp_path):
    path = tmp_path / "header.pgn"
    path.write_text(_base().split("\n\n")[0])
    out = tmp_path / "out"
    result = run_pipeline(path, out, "fixture://header")
    assert result["valid_silver"] == 0 and result["quarantined"] == 1
    assert "missing move text" in _read(out / "quarantine/games.jsonl")[0]["errors"]


def test_concatenated_zstd_frames_and_stale_receipt(tmp_path):
    compressed = tmp_path / "source.zst"
    payload = FIXTURE.read_bytes()
    midpoint = len(payload) // 2
    compressor = zstandard.ZstdCompressor()
    compressed.write_bytes(
        compressor.compress(payload[:midpoint]) + compressor.compress(payload[midpoint:])
    )
    out = tmp_path / "out"
    assert run_pipeline(compressed, out, "fixture://multiple")["valid_silver"] == 1
    sample = tmp_path / "sample.pgn"
    download_bounded_sample(compressed.as_uri(), sample, 3)
    sample.write_text(sample.read_text().replace("alice", "changed"))
    with pytest.raises(ValueError, match="receipt does not match"):
        run_pipeline(sample, tmp_path / "stale", compressed.as_uri())
    assert not (tmp_path / "stale").exists()
