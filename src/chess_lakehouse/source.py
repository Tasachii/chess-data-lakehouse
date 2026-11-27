from __future__ import annotations

import hashlib
import json
import ssl
import tempfile
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
    output = Path(destination)
    output.parent.mkdir(parents=True, exist_ok=True)
    tls_context = ssl.create_default_context(cafile=certifi.where())
    checksum = hashlib.sha256()
    count = size = 0
    # Stage the sample so short or broken downloads never replace published bytes.
    with tempfile.TemporaryDirectory(prefix=".sample-download-", dir=output.parent) as temporary:
        staged = Path(temporary) / output.name
        with (
            urlopen(  # noqa: S310 - caller selects the source URL
                request, timeout=60, context=tls_context
            ) as response,
            staged.open("wb") as handle,
        ):
            for game in _stream_pgn_games(decoded_lines(response)):
                record = (("\n" if count else "") + game + "\n").encode("utf-8")
                handle.write(record)
                checksum.update(record)
                size += len(record)
                count += 1
                if count >= max_games:
                    break
        if count < max_games:
            raise ValueError(f"Source ended after {count} records; requested {max_games}")
        receipt = {
            **source_metadata(source_url),
            "source_url": source_url,
            "source_sha256": checksum.hexdigest(),
            "retrieved_at_utc": datetime.now(UTC).isoformat(),
            "requested_max_games": max_games,
            "games_written": count,
            "bytes_written": size,
        }
        staged_receipt = receipt_path(staged)
        staged_receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        staged.replace(output)
        staged_receipt.replace(receipt_path(output))
    return {"games_written": count, "bytes_written": size}
