import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from chess_lakehouse.local_pipeline import run_pipeline

ROOT = Path(__file__).resolve().parents[1]


def test_checked_in_snapshot_contains_only_grouped_metrics():
    snapshot = json.loads((ROOT / "docs/assets/snapshot.json").read_text())
    counts = snapshot["counts"]
    outcomes = snapshot["outcomes"]
    assert snapshot["schema_version"] == 2
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


def test_dashboard_snapshot_contains_verified_million_game_opening_families():
    snapshot = json.loads((ROOT / "docs/assets/snapshot.json").read_text())
    assert snapshot["source"]["requested_games"] == 1_000_000
    assert snapshot["counts"] == {"bronze": 1_000_000, "silver": 998_976, "quarantine": 1024}
    assert snapshot["source"]["sha256"] == (
        "6050321afa4d758518168b63f58b5845f2dec4947a1ce92c6d6b25b6c70905a0"
    )
    assert snapshot["outcomes"] == {"white_wins": 497_294, "black_wins": 463_906, "draws": 37_776}
    expected = {
        "Italian Game": (40_544, 20_212, 18_831, 1501),
        "Ruy Lopez": (16_814, 8421, 7402, 991),
        "Sicilian Defense": (96_289, 46_512, 46_148, 3629),
        "Caro-Kann Defense": (56_896, 27_431, 27_282, 2183),
    }
    for family, counts in expected.items():
        rows = [row for row in snapshot["groups"]["opening"] if family in row["label"]]
        assert (
            tuple(
                sum(row[key] for row in rows)
                for key in ("games", "white_wins", "black_wins", "draws")
            )
            == counts
        )


def test_context_and_extra_marts_reconcile_to_full_snapshot():
    snapshot = json.loads((ROOT / "docs/assets/snapshot.json").read_text())
    context = snapshot["marts"]["opening_context"]
    columns = [
        "opening",
        "eco",
        "rating_bucket",
        "speed",
        "games",
        "white_wins",
        "black_wins",
        "draws",
    ]
    assert context["columns"] == columns
    assert len(context["rows"]) == 42_618
    rows = [dict(zip(columns, row, strict=True)) for row in context["rows"]]
    metrics = ("games", "white_wins", "black_wins", "draws")
    assert all(row["games"] == sum(row[key] for key in metrics[1:]) for row in rows)
    for dimension in ("opening", "rating_bucket", "speed"):
        rolled = defaultdict(lambda: dict.fromkeys(metrics, 0))
        for row in rows:
            for key in metrics:
                rolled[row[dimension]][key] += row[key]
        expected = {
            row["label"]: {key: row[key] for key in metrics}
            for row in snapshot["groups"][dimension]
        }
        assert dict(rolled) == expected
    for table in ("rating_gap_metrics", "termination_metrics", "length_metrics"):
        rows = snapshot["marts"][table]
        assert sum(row["games"] for row in rows) == snapshot["counts"]["silver"]
        assert {key: sum(row[key] for row in rows) for key in metrics[1:]} == snapshot["outcomes"]
    assert snapshot["coverage"] == {
        "eco_known": 997_485,
        "termination_known": 998_976,
        "length_known": 997_485,
        "total_plies": 65_810_996,
    }
    length = snapshot["marts"]["length_metrics"]
    assert sum(row["total_plies"] for row in length) == snapshot["coverage"]["total_plies"]
    assert sum(row["known_length_games"] for row in length) == snapshot["coverage"]["length_known"]
    assert next(row for row in length if row["label"] == "Unknown")["games"] == 1491


def test_million_manifest_preserves_verified_cloud_prefix():
    assets = ROOT / "docs/assets"
    baseline = json.loads((assets / "sample-chunks-manifest.json").read_text())
    expanded = json.loads((assets / "sample-chunks-1m-manifest.json").read_text())
    snapshot_bytes = (assets / "snapshot.json").read_bytes()
    snapshot = json.loads(snapshot_bytes)
    assert len(expanded["parts"]) == 100
    assert expanded["total_games"] == 1_000_000
    assert expanded["parts"][:10] == baseline["parts"]
    assert expanded["parent_source_sha256"] == snapshot["source"]["sha256"]
    assert expanded["source_url"] == snapshot["source"]["url"]
    assert expanded["retrieved_at_utc"] == snapshot["source"]["retrieved_at_utc"]
    assert sum(part["games"] for part in expanded["parts"]) == 1_000_000
    assert sum(part["bytes"] for part in expanded["parts"]) + 99 == 2_321_066_081
    for i, part in enumerate(expanded["parts"], 1):
        assert part["file"] == f"part-{i:03d}.pgn"
        assert part["games"] == 10_000
        assert part["first_record"] == (i - 1) * 10_000 + 1
        assert part["last_record"] == i * 10_000
        assert len(bytes.fromhex(part["source_sha256"])) == 32
    assert hashlib.sha256(snapshot_bytes).hexdigest() == (
        "0bc49de7258a0424657a469074be3963c207f7ef5f7d1b110d8b764177988c5a"
    )
    report = json.loads((assets / "scale-run-report.json").read_text())
    checkpoints = report["checkpoints"]
    assert [row["source"]["requested_games"] for row in checkpoints] == [
        100_000,
        250_000,
        1_000_000,
    ]
    assert checkpoints[0]["snapshot_sha256"] == (
        "71c078b99c9ec5f2177166704fd5c8a99512cca8082d190c9b2bbbcca94bb239"
    )
    assert checkpoints[-1]["counts"] == snapshot["counts"]
    assert checkpoints[-1]["outcomes"] == snapshot["outcomes"]
    assert sum(checkpoints[-1]["gold_groups"].values()) == 45_332
    assert checkpoints[-1]["snapshot_sha256"] == hashlib.sha256(snapshot_bytes).hexdigest()


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
