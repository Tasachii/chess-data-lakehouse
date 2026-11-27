import csv
import json
from pathlib import Path

from chess_lakehouse.local_pipeline import run_pipeline

FIXTURE = Path(__file__).parent / "fixtures" / "sample.pgn"


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def test_pipeline_quarantines_invalid_rows_and_is_idempotent(tmp_path):
    output = tmp_path / "lakehouse"

    first = run_pipeline(FIXTURE, output, source_url="fixture://sample.pgn")
    second = run_pipeline(FIXTURE, output, source_url="fixture://sample.pgn")

    assert first == {"input_games": 3, "inserted_bronze": 2, "valid_silver": 1, "quarantined": 1}
    assert second == {"input_games": 3, "inserted_bronze": 0, "valid_silver": 1, "quarantined": 1}
    assert len(_jsonl(output / "bronze" / "games.jsonl")) == 2
    assert len(_jsonl(output / "silver" / "games.jsonl")) == 1
    quarantine = _jsonl(output / "quarantine" / "games.jsonl")
    assert quarantine[0]["errors"] == [
        "result must be one of 1-0, 0-1, 1/2-1/2",
        "white_elo must be an integer",
        "time_control must use initial+increment seconds",
    ]


def test_pipeline_writes_inspectable_gold_and_provenance_manifest(tmp_path):
    output = tmp_path / "lakehouse"

    run_pipeline(FIXTURE, output, source_url="fixture://sample.pgn")

    with (output / "gold" / "opening_metrics.csv").open(newline="") as handle:
        assert list(csv.DictReader(handle)) == [
            {
                "opening": "Sicilian Defense",
                "games": "1",
                "white_wins": "1",
                "draws": "0",
                "white_win_rate": "1.0",
            }
        ]
    manifest = json.loads((output / "manifests" / "latest.json").read_text())
    assert manifest["source_url"] == "fixture://sample.pgn"
    assert manifest["source_sha256"]
    assert manifest["counts"]["valid_silver"] == 1


def test_incremental_batch_preserves_existing_games_and_quarantines_missing_fields(tmp_path):
    output = tmp_path / "lakehouse"
    run_pipeline(FIXTURE, output, source_url="fixture://sample.pgn")
    from chess_lakehouse.transform import parse_pgn_games

    original = next(iter(parse_pgn_games(FIXTURE.read_text())))
    missing = original.replace("alpha001", "missing1").replace('[BlackElo "1700"]\n', "")
    batch = tmp_path / "batch.pgn"
    batch.write_text(original + "\n\n" + missing + "\n")
    first = run_pipeline(batch, output, source_url="fixture://incremental.pgn")
    assert first == {"input_games": 2, "inserted_bronze": 1, "valid_silver": 1, "quarantined": 2}
    bronze = output / "bronze" / "games.jsonl"
    before = bronze.read_bytes()
    assert run_pipeline(batch, output, "fixture://incremental.pgn")["inserted_bronze"] == 0
    assert bronze.read_bytes() == before
    assert {row["game_id"] for row in _jsonl(bronze)} == {"alpha001", "bad00001", "missing1"}
    assert {row["game_id"] for row in _jsonl(output / "quarantine" / "games.jsonl")} == {
        "bad00001",
        "missing1",
    }
