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


def test_download_keeps_memory_bounded_and_preserves_bytes(tmp_path, monkeypatch):
    import hashlib
    import json
    import tracemalloc

    from chess_lakehouse import source

    compressed = tmp_path / "source.pgn.zst"
    compressed.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes()))
    destination = tmp_path / "bounded.pgn"
    total = 5000

    def games(_lines):
        for i in range(total + 1):
            yield f"[Event \"{i}\"]\n\n{'x' * 4096} 1-0"

    monkeypatch.setattr(source, "_stream_pgn_games", games)
    tracemalloc.start()
    try:
        result = download_bounded_sample(compressed.as_uri(), destination, total)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 8 * 1024 * 1024, f"Download buffered {peak} bytes"
    expected = hashlib.sha256()
    for i, game in enumerate(games(None)):
        if i == total:
            break
        expected.update((("\n\n" if i else "") + game).encode())
    expected.update(b"\n")
    with destination.open("rb") as handle:
        assert hashlib.file_digest(handle, "sha256").hexdigest() == expected.hexdigest()
    receipt = json.loads(source.receipt_path(destination).read_text())
    assert receipt["source_sha256"] == expected.hexdigest()
    assert receipt["bytes_written"] == destination.stat().st_size == result["bytes_written"]
    assert result["games_written"] == total


def test_short_redownload_preserves_previous_sample_and_receipt(tmp_path):
    import pytest

    from chess_lakehouse.lineage import receipt_path

    compressed = tmp_path / "source.pgn.zst"
    compressed.write_bytes(zstandard.ZstdCompressor().compress(FIXTURE.read_bytes()))
    destination = tmp_path / "bounded.pgn"
    download_bounded_sample(compressed.as_uri(), destination, 2)
    before = destination.read_bytes(), receipt_path(destination).read_bytes()
    with pytest.raises(ValueError, match="Source ended"):
        download_bounded_sample(compressed.as_uri(), destination, 4)
    assert (destination.read_bytes(), receipt_path(destination).read_bytes()) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        [compressed.name, destination.name, receipt_path(destination).name]
    )
