from __future__ import annotations

import hashlib
import json
import ssl
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

import certifi

from chess_lakehouse.compression import decoded_lines
from chess_lakehouse.lineage import receipt_path, source_metadata
from chess_lakehouse.transform import stream_pgn_games as _stream_pgn_games


def download_bounded_sample(
    source_url: str, destination: str | Path, max_games: int
) -> dict[str, int]:
    """Stream a zstd PGN source and stop after complete `max_games` records."""
    if max_games < 1:
        raise ValueError("max_games must be positive")
    request = Request(source_url, headers={"User-Agent": "chess-data-lakehouse/0.1"})
    games: list[str] = []
    tls_context = ssl.create_default_context(cafile=certifi.where())
    with urlopen(  # noqa: S310 - caller selects the source URL
        request, timeout=60, context=tls_context
    ) as response:
        for game in _stream_pgn_games(decoded_lines(response)):
            games.append(game)
            if len(games) >= max_games:
                break
    if len(games) < max_games:
        raise ValueError(f"Source ended after {len(games)} records; requested {max_games}")
    output = Path(destination)
    payload = ("\n\n".join(games) + "\n").encode("utf-8")
    receipt = {
        **source_metadata(source_url),
        "source_url": source_url,
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "requested_max_games": max_games,
        "games_written": len(games),
        "bytes_written": len(payload),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(payload)
    receipt_path(output).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return {"games_written": len(games), "bytes_written": len(payload)}
