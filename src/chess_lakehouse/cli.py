from __future__ import annotations

import argparse
import json
from pathlib import Path

from chess_lakehouse.local_pipeline import run_pipeline
from chess_lakehouse.source import download_bounded_sample


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local Lichess lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)

    download = commands.add_parser("download", help="download a bounded sample from PGN.zst")
    download.add_argument("--url", required=True)
    download.add_argument("--output", type=Path, required=True)
    download.add_argument("--max-games", type=int, default=100)

    run = commands.add_parser("run", help="build local bronze/silver/gold outputs")
    run.add_argument("--input", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--source-url", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "download":
        result = download_bounded_sample(args.url, args.output, args.max_games)
    else:
        result = run_pipeline(args.input, args.output, args.source_url)
    print(json.dumps(result, indent=2, sort_keys=True))
