"""Export aggregate-only, verified Gold metrics for the static dashboard."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

DIMENSIONS = {
    "opening": "opening_metrics.csv",
    "rating_bucket": "rating_metrics.csv",
    "speed": "time_control_metrics.csv",
}
SOURCE_PATTERN = re.compile(
    r"https://database\.lichess\.org/standard/lichess_db_standard_rated_(\d{4}-\d{2})\.pgn\.zst"
)


def export_snapshot(lakehouse: Path, destination: Path) -> dict:
    manifest = json.loads((lakehouse / "manifests/latest.json").read_text())
    source_match = SOURCE_PATTERN.fullmatch(manifest["source_url"])
    if not source_match or manifest["source_kind"] != "lichess_public":
        raise ValueError("The published dashboard requires a real Lichess standard-rated sample")
    bound = manifest["requested_max_games"]
    if not bound or not manifest["retrieved_at_utc"]:
        raise ValueError("The source must have a verified download receipt")
    counts = manifest["cumulative_counts"]
    if counts["bronze"] != bound or counts["silver"] + counts["quarantine"] != bound:
        raise ValueError("Use a fresh single-sample lakehouse; counts must reconcile to the bound")
    history = [json.loads(p.read_text()) for p in (lakehouse / "manifests/runs").glob("*.json")]
    if not history or {run["batch_id"] for run in history} != {manifest["batch_id"]}:
        raise ValueError("Dashboard export requires one distinct source batch")

    # Read only fields required to verify grouped counts and measure the UTC time range.
    # Never serialize entire Silver records or Bronze/quarantine rows.
    expected = {key: defaultdict(Counter) for key in DIMENSIONS}
    results: Counter = Counter()
    played_at = []
    with (lakehouse / "silver/games.jsonl").open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            outcome = row["result"]
            results[outcome] += 1
            played_at.append(row["played_at"])
            for dim in DIMENSIONS:
                expected[dim][row[dim]][outcome] += 1
    if len(played_at) != counts["silver"] or not played_at:
        raise ValueError("Silver count does not match the manifest or is empty")

    groups = {}
    for dim, filename in DIMENSIONS.items():
        rows = []
        with (lakehouse / "gold" / filename).open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                label = row[dim]
                wins = int(row["white_wins"])
                draws = int(row["draws"])
                games = int(row["games"])
                black_wins = games - wins - draws
                actual = {"1-0": wins, "0-1": black_wins, "1/2-1/2": draws}
                if Counter(actual) != expected[dim][label] or games <= 0:
                    raise ValueError(f"Gold mismatch for {dim}: {label}")
                if abs(float(row["white_win_rate"]) - wins / games) > 0.0000500001:
                    raise ValueError(f"Gold rate mismatch for {dim}: {label}")
                rows.append(
                    {
                        "label": label,
                        "games": games,
                        "white_wins": wins,
                        "black_wins": black_wins,
                        "draws": draws,
                    }
                )
        if len(rows) != len(expected[dim]) or {row["label"] for row in rows} != set(expected[dim]):
            raise ValueError(f"Gold groups do not match Silver: {dim}")
        if sum(row["games"] for row in rows) != counts["silver"]:
            raise ValueError(f"Gold total mismatch: {dim}")
        groups[dim] = sorted(rows, key=lambda row: row["label"])

    snapshot = {
        "schema_version": 1,
        "source": {
            "url": manifest["source_url"],
            "month": source_match.group(1),
            "license": manifest["source_license"],
            "license_url": manifest["source_license_url"],
            "sha256": manifest["source_sha256"],
            "retrieved_at_utc": manifest["retrieved_at_utc"],
            "first_game_utc": min(played_at),
            "last_game_utc": max(played_at),
            "requested_games": bound,
            "pipeline_version": manifest["pipeline_version"],
        },
        "counts": counts,
        "outcomes": {
            "white_wins": results["1-0"],
            "black_wins": results["0-1"],
            "draws": results["1/2-1/2"],
        },
        "groups": groups,
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lakehouse", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("docs/assets/snapshot.json"))
    args = parser.parse_args()
    result = export_snapshot(args.lakehouse, args.output)
    print(
        json.dumps(
            {
                "counts": result["counts"],
                "groups": {k: len(v) for k, v in result["groups"].items()},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
