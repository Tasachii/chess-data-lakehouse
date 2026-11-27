"""Split a receipt-verified PGN into bounded files and an integrity manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

from chess_lakehouse.lineage import input_receipt, receipt_path
from chess_lakehouse.transform import stream_pgn_games


def split_sample(source: Path, destination: Path, games_per_file: int) -> dict:
    if games_per_file < 1:
        raise ValueError("games_per_file must be positive")
    if destination.exists():
        raise ValueError("Choose a new output directory for the chunks")
    with source.open("rb") as handle:
        source_hash = hashlib.file_digest(handle, "sha256").hexdigest()
    receipt = json.loads(receipt_path(source).read_text(encoding="utf-8"))
    provenance = input_receipt(source, receipt["source_url"], source_hash)
    total = provenance["requested_max_games"]
    if total is None or total % games_per_file:
        raise ValueError("The receipt bound must divide evenly into the requested chunk size")
    if source.stat().st_size != receipt["bytes_written"]:
        raise ValueError("Input size does not match its receipt")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
        staging = Path(temporary) / "parts"
        staging.mkdir()
        manifest = {
            "parent_source_sha256": source_hash,
            "source_url": receipt["source_url"],
            "retrieved_at_utc": receipt["retrieved_at_utc"],
            "total_games": total,
            "games_per_file": games_per_file,
            "parts": [],
        }
        reconstructed = hashlib.sha256()
        games = []
        with source.open(encoding="utf-8") as handle:
            for game in stream_pgn_games(handle):
                games.append(game)
                if len(games) != games_per_file:
                    continue
                number = len(manifest["parts"]) + 1
                path = staging / f"part-{number:03d}.pgn"
                payload = ("\n\n".join(games) + "\n").encode("utf-8")
                path.write_bytes(payload)
                if number > 1:
                    reconstructed.update(b"\n")
                reconstructed.update(payload)
                part_hash = hashlib.sha256(payload).hexdigest()
                part = {
                    "file": path.name,
                    "source_sha256": part_hash,
                    "games": len(games),
                    "bytes": len(payload),
                    "first_record": (number - 1) * games_per_file + 1,
                    "last_record": number * games_per_file,
                }
                manifest["parts"].append(part)
                receipt_path(path).write_text(
                    json.dumps(
                        {
                            **receipt,
                            "source_sha256": part_hash,
                            "requested_max_games": len(games),
                            "games_written": len(games),
                            "bytes_written": len(payload),
                            "parent_source_sha256": source_hash,
                            "parent_requested_max_games": total,
                            "chunk_number": number,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n"
                )
                games.clear()
        if games or sum(part["games"] for part in manifest["parts"]) != total:
            raise ValueError("Chunk counts do not match the parent receipt")
        if reconstructed.hexdigest() != source_hash:
            raise ValueError(
                "Reconstructed chunks do not match the source hash; use a canonical download"
            )
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        staging.rename(destination)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--games-per-file", type=int, default=10_000)
    args = parser.parse_args()
    print(json.dumps(split_sample(args.input, args.output, args.games_per_file), indent=2))


if __name__ == "__main__":
    main()
