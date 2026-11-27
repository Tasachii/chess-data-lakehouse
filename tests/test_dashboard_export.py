import json
import subprocess
import sys
from pathlib import Path

from chess_lakehouse.local_pipeline import run_pipeline

ROOT = Path(__file__).resolve().parents[1]


def test_checked_in_snapshot_contains_only_grouped_metrics():
    snapshot = json.loads((ROOT / "docs/assets/snapshot.json").read_text())
    counts = snapshot["counts"]
    outcomes = snapshot["outcomes"]
    assert snapshot["schema_version"] == 1
    assert counts["bronze"] == snapshot["source"]["requested_games"]
    assert counts["bronze"] == counts["silver"] + counts["quarantine"]
    assert sum(outcomes.values()) == counts["silver"]
    assert snapshot["source"]["license"] == "CC0-1.0"
    assert set(snapshot["groups"]) == {"opening", "rating_bucket", "speed"}
    for groups in snapshot["groups"].values():
        assert len({row["label"] for row in groups}) == len(groups)
        for row in groups:
            assert set(row) == {"label", "games", "white_wins", "black_wins", "draws"}
            assert row["games"] == sum(row[key] for key in outcomes)
        assert sum(row["games"] for row in groups) == counts["silver"]
        assert {key: sum(row[key] for row in groups) for key in outcomes} == outcomes


def test_export_rejects_synthetic_fixture(tmp_path):
    lakehouse = tmp_path / "lakehouse"
    destination = tmp_path / "snapshot.json"
    run_pipeline(ROOT / "tests/fixtures/sample.pgn", lakehouse, "fixture://sample.pgn")
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/export_dashboard.py"),
            "--lakehouse",
            str(lakehouse),
            "--output",
            str(destination),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "requires a real Lichess" in result.stderr
    assert not destination.exists()
