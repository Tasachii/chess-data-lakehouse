from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Any

TAG_PATTERN = re.compile(r'^\[([^ ]+) "(.*)"\]$')
LICHESS_ID_PATTERN = re.compile(r"^https?://lichess\.org/(?:game/)?([A-Za-z0-9]{8})(?:/.*)?$")
VALID_RESULTS = {"1-0", "0-1", "1/2-1/2"}


def stream_pgn_games(lines: Iterable[str]) -> Iterable[str]:
    """Split at each Event header, including after a malformed header-only record."""
    current: list[str] = []
    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        if line.startswith("[Event ") and current:
            yield "\n".join(current).strip()
            current = []
        if line.strip() or current:
            current.append(line)
    if current:
        yield "\n".join(current).strip()


def parse_pgn_games(text: str) -> Iterable[str]:
    """Yield record blocks using the same boundaries as bounded ingestion."""
    return stream_pgn_games(text.splitlines())


def _tags(raw_pgn: str) -> dict[str, str]:
    tags: dict[str, str] = {}
    for line in raw_pgn.splitlines():
        match = TAG_PATTERN.match(line)
        if match:
            tags[match.group(1)] = match.group(2)
    return tags


def stable_game_id(raw_pgn: str) -> str:
    tags = _tags(raw_pgn)
    match = LICHESS_ID_PATTERN.match(tags.get("Site", ""))
    if match:
        return match.group(1)
    canonical = "\n".join(line.strip() for line in raw_pgn.splitlines() if line.strip())
    return "sha256-" + hashlib.sha256(canonical.encode()).hexdigest()


def _parse_rating(value: str | None, field: str, errors: list[str]) -> int | None:
    try:
        rating = int(value or "")
    except ValueError:
        errors.append(f"{field} must be an integer")
        return None
    if not 100 <= rating <= 4000:
        errors.append(f"{field} must be between 100 and 4000")
        return None
    return rating


def _parse_time_control(value: str | None, errors: list[str]) -> tuple[int, int] | None:
    match = re.fullmatch(r"(\d+)\+(\d+)", value or "")
    if not match:
        errors.append("time_control must use initial+increment seconds")
        return None
    return int(match.group(1)), int(match.group(2))


def _speed(initial: int, increment: int) -> str:
    estimated_seconds = initial + 40 * increment
    if estimated_seconds < 180:
        return "bullet"
    if estimated_seconds < 600:
        return "blitz"
    if estimated_seconds < 1800:
        return "rapid"
    return "classical"


def normalize_game(raw_pgn: str) -> tuple[dict[str, Any] | None, list[str]]:
    tags = _tags(raw_pgn)
    errors: list[str] = []
    result = tags.get("Result")
    if result not in VALID_RESULTS:
        errors.append("result must be one of 1-0, 0-1, 1/2-1/2")
    white_elo = _parse_rating(tags.get("WhiteElo"), "white_elo", errors)
    black_elo = _parse_rating(tags.get("BlackElo"), "black_elo", errors)
    time_control = _parse_time_control(tags.get("TimeControl"), errors)
    try:
        played_at = datetime.strptime(
            f"{tags.get('UTCDate', '')} {tags.get('UTCTime', '')}", "%Y.%m.%d %H:%M:%S"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        errors.append("UTCDate and UTCTime must define a valid UTC timestamp")
        played_at = None
    if not any(line.strip() and not line.lstrip().startswith("[") for line in raw_pgn.splitlines()):
        errors.append("missing move text")
    if errors:
        return None, errors

    assert white_elo is not None and black_elo is not None and time_control is not None
    average_rating = (white_elo + black_elo) // 2
    bucket_start = average_rating // 200 * 200
    return {
        "game_id": stable_game_id(raw_pgn),
        "played_at": played_at,
        "white_player": tags.get("White") or "unknown",
        "black_player": tags.get("Black") or "unknown",
        "white_elo": white_elo,
        "black_elo": black_elo,
        "result": result,
        "opening": tags.get("Opening") or "Unknown",
        "initial_seconds": time_control[0],
        "increment_seconds": time_control[1],
        "speed": _speed(*time_control),
        "rating_bucket": f"{bucket_start}-{bucket_start + 199}",
    }, []


def aggregate_gold(games: Iterable[dict[str, Any] | None]) -> dict[str, list[dict[str, Any]]]:
    dimensions = {
        "opening_metrics": "opening",
        "rating_metrics": "rating_bucket",
        "time_control_metrics": "speed",
    }
    output: dict[str, list[dict[str, Any]]] = {}
    valid_games = [game for game in games if game is not None]
    for table, dimension in dimensions.items():
        grouped: dict[str, dict[str, Any]] = defaultdict(
            lambda dimension=dimension: {dimension: "", "games": 0, "white_wins": 0, "draws": 0}
        )
        for game in valid_games:
            key = game[dimension]
            row = grouped[key]
            row[dimension] = key
            row["games"] += 1
            row["white_wins"] += int(game["result"] == "1-0")
            row["draws"] += int(game["result"] == "1/2-1/2")
        rows = [grouped[key] for key in sorted(grouped)]
        for row in rows:
            row["white_win_rate"] = round(row["white_wins"] / row["games"], 4)
        output[table] = rows
    return output
