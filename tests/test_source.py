from pathlib import Path

import zstandard

from chess_lakehouse.source import download_bounded_sample
from chess_lakehouse.transform import parse_pgn_games

FIXTURE = Path(__file__).parent / "fixtures" / "sample.pgn"


def test_download_stops_after_requested_number_of_complete_games(tmp_path):
    compressed = tmp_path / "source.pgn.zst"
    compressed.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes()))
    destination = tmp_path / "bounded.pgn"

    result = download_bounded_sample(compressed.as_uri(), destination, max_games=2)

    games = list(parse_pgn_games(destination.read_text()))
    assert result["games_written"] == 2
    assert len(games) == 2
    assert games[-1].endswith("1-0")
