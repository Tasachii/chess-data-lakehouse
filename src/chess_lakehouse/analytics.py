"""Optional PGN fields and descriptive analytics shared by the lakehouse paths."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any

ECO_PATTERN = r"^[A-E][0-9]{2}$"
TAG_LINE_PATTERN = r'^\[[^ ]+ ".*"\]$'
COMMENT_PATTERN = r"\{[^{}]*\}"
MOVE_NUMBER_PATTERN = r"\b\d+\.(?:\.\.)?"
UNSUPPORTED_MOVE_PATTERN = r"[{}\[\]();]"
SAN_PATTERN = r"^(?:O-O(?:-O)?|[KQRBN]?[a-h]?[1-8]?x?[a-h][1-8](?:=[QRBN])?)[+#]?[!?]*$"
TERMINATIONS = (
    "Normal",
    "Time forfeit",
    "Abandoned",
    "Insufficient material",
    "Rules infraction",
    "Adjudication",
    "Death",
    "Emergency",
    "Unterminated",
)
GAP_LIMITS = (
    (-400, "≤ -400"),
    (-200, "-399 to -200"),
    (-100, "-199 to -100"),
    (99, "-99 to +99"),
    (199, "+100 to +199"),
    (399, "+200 to +399"),
)
LENGTH_LIMITS = (
    (20, "1–20"),
    (40, "21–40"),
    (60, "41–60"),
    (80, "61–80"),
    (100, "81–100"),
    (120, "101–120"),
)
ANALYTICS_FIELDS = (
    "eco",
    "termination",
    "rating_gap",
    "rating_gap_bucket",
    "plies",
    "length_bucket",
)
ANALYTICS_DIMENSIONS = {
    "opening_context": ("opening", "eco", "rating_bucket", "speed"),
    "rating_gap_metrics": ("rating_gap_bucket",),
    "termination_metrics": ("termination",),
    "length_metrics": ("length_bucket",),
}


def mainline_plies(raw_pgn: str, result: str) -> int | None:
    """Count SAN-shaped half-moves only for a simple, consistent mainline.

    This is a lexical count, not a chess legality check. Unsupported notation,
    variations, missing results and result-only records have unknown length.
    """
    body = " ".join(
        line for line in raw_pgn.splitlines() if not re.fullmatch(TAG_LINE_PATTERN, line)
    )
    stripped = re.sub(COMMENT_PATTERN, " ", body)
    if re.search(UNSUPPORTED_MOVE_PATTERN, stripped):
        return None
    words = re.sub(MOVE_NUMBER_PATTERN, " ", stripped).split()
    if len(words) < 2 or words[-1] != result:
        return None
    if not all(re.fullmatch(SAN_PATTERN, word) for word in words[:-1]):
        return None
    return len(words) - 1


def bucket(value: int, limits: tuple[tuple[int, str], ...], remainder: str) -> str:
    return next((label for upper, label in limits if value <= upper), remainder)


def optional_fields(
    raw_pgn: str, tags: dict[str, str], white_elo: int, black_elo: int
) -> dict[str, Any]:
    eco = tags.get("ECO", "")
    termination = tags.get("Termination", "")
    gap = white_elo - black_elo
    plies = mainline_plies(raw_pgn, tags["Result"])
    return {
        "eco": eco if re.fullmatch(ECO_PATTERN, eco) else "Unknown",
        "termination": termination if termination in TERMINATIONS else "Unknown",
        "rating_gap": gap,
        "rating_gap_bucket": bucket(gap, GAP_LIMITS, "≥ +400"),
        "plies": plies,
        "length_bucket": bucket(plies, LENGTH_LIMITS, "121+") if plies is not None else "Unknown",
    }


def rounded_rate(wins: int, games: int) -> float:
    """Round an exact count ratio to four decimals with half-even ties."""
    return float(
        (Decimal(wins) / Decimal(games)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_EVEN)
    )


def aggregate_analytics(games: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    rows = list(games)
    output = {}
    for table, dimensions in ANALYTICS_DIMENSIONS.items():
        counts = defaultdict(lambda: {"games": 0, "white_wins": 0, "draws": 0})
        for game in rows:
            key = tuple(game[dim] for dim in dimensions)
            row = counts[key]
            row["games"] += 1
            row["white_wins"] += int(game["result"] == "1-0")
            row["draws"] += int(game["result"] == "1/2-1/2")
            if table == "length_metrics":
                row["known_length_games"] = row.get("known_length_games", 0) + int(
                    game["plies"] is not None
                )
                row["total_plies"] = row.get("total_plies", 0) + (game["plies"] or 0)
        output[table] = [
            {
                **dict(zip(dimensions, key, strict=True)),
                **counts[key],
                "white_win_rate": rounded_rate(counts[key]["white_wins"], counts[key]["games"]),
            }
            for key in sorted(counts)
        ]
    return output
