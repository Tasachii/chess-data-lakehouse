"""Verify optional PGN analytics and their aggregation grain."""

import csv
import json
from pathlib import Path

import pytest

from chess_lakehouse.analytics import ANALYTICS_DIMENSIONS, aggregate_analytics, mainline_plies
from chess_lakehouse.local_pipeline import run_pipeline
from chess_lakehouse.transform import aggregate_gold, normalize_game, parse_pgn_games

FIXTURE = Path(__file__).parent / "fixtures/sample.pgn"


def test_optional_analytics_and_signed_gap():
    raw = next(iter(parse_pgn_games(FIXTURE.read_text())))
    enriched = raw.replace(
        '[Opening "Sicilian Defense"]',
        '[Opening "Sicilian Defense"]\n[ECO "B20"]\n[Termination "Time forfeit"]',
    )
    game, errors = normalize_game(enriched)
    assert not errors and game is not None
    assert {
        key: game[key]
        for key in (
            "eco",
            "termination",
            "rating_gap",
            "rating_gap_bucket",
            "plies",
            "length_bucket",
        )
    } == {
        "eco": "B20",
        "termination": "Time forfeit",
        "rating_gap": -200,
        "rating_gap_bucket": "-399 to -200",
        "plies": 4,
        "length_bucket": "1–20",
    }
    unknown, errors = normalize_game(raw.replace("1. e4 c5 2. Nf3 d6 1-0", "1-0"))
    assert not errors and unknown is not None
    assert unknown["eco"] == unknown["termination"] == unknown["length_bucket"] == "Unknown"
    assert unknown["plies"] is None


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("1. e4 { [%clk 0:05:00] } 1... c5 { [%eval 0.1] } 2. Nf3 d6 1-0", 4),
        ("1. e4 e5 2. Nf3 1-0", 3),
        ("1. e4 (1. d4) e5 1-0", None),
        ("1. e4 { incomplete e5 1-0", None),
        ("1. e4 e5 0-1", None),
        ("1-0", None),
        ("1. e4 bogus 1-0", None),
    ],
)
def test_mainline_plies_is_nullable_for_unsupported_text(body, expected):
    assert mainline_plies(body, "1-0") == expected


def test_extended_marts_reconcile_and_local_version_guard(tmp_path):
    out = tmp_path / "lakehouse"
    first = run_pipeline(FIXTURE, out, "fixture://analytics")
    assert first["valid_silver"] == 1
    silver = [json.loads(line) for line in (out / "silver/games.jsonl").read_text().splitlines()]
    marts = aggregate_analytics(silver)
    assert set(marts) == set(ANALYTICS_DIMENSIONS)
    for table, dimensions in ANALYTICS_DIMENSIONS.items():
        assert sum(row["games"] for row in marts[table]) == 1
        rows = list(csv.DictReader((out / "gold" / f"{table}.csv").open()))
        assert len(rows) == 1 and all(key in rows[0] for key in dimensions)
        assert rows[0]["white_wins"] == "1" and rows[0]["draws"] == "0"
    assert marts["length_metrics"][0]["total_plies"] == 4
    assert marts["length_metrics"][0]["known_length_games"] == 1
    manifest = out / "manifests/latest.json"
    previous = json.loads(manifest.read_text())
    previous["pipeline_version"] = "0.2.0"
    manifest.write_text(json.dumps(previous))
    before = (out / "bronze/games.jsonl").read_bytes()
    with pytest.raises(ValueError, match="fresh output"):
        run_pipeline(FIXTURE, out, "fixture://analytics")
    assert (out / "bronze/games.jsonl").read_bytes() == before


def test_exact_half_tie_rounding_preserves_counts():
    raw = next(iter(parse_pgn_games(FIXTURE.read_text())))
    normalized, _ = normalize_game(raw)
    assert normalized is not None
    rows = [{**normalized, "result": "1-0" if index < 77 else "0-1"} for index in range(160)]
    for result in (
        aggregate_gold(rows)["opening_metrics"],
        aggregate_analytics(rows)["opening_context"],
    ):
        assert result[0]["games"] == 160 and result[0]["white_wins"] == 77
        assert result[0]["white_win_rate"] == 0.4812
