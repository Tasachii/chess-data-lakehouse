import hashlib
import json
import subprocess
import sys
from pathlib import Path

import zstandard

from chess_lakehouse.source import download_bounded_sample

ROOT = Path(__file__).resolve().parents[1]


def test_sample_chunks_preserve_receipt_counts_and_bytes(tmp_path):
    fixture = ROOT / "tests/fixtures/sample.pgn"
    packed = tmp_path / "source.zst"
    packed.write_bytes(zstandard.ZstdCompressor().compress(fixture.read_bytes()))
    sample = tmp_path / "sample.pgn"
    download_bounded_sample(packed.as_uri(), sample, 3)
    out = tmp_path / "parts"
    command = [
        sys.executable,
        str(ROOT / "scripts/split_sample.py"),
        "--input",
        str(sample),
        "--output",
        str(out),
        "--games-per-file",
        "1",
    ]
    subprocess.run(command, check=True, capture_output=True)
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["total_games"] == 3
    assert len(manifest["parts"]) == 3
    assert manifest["parent_source_sha256"] == hashlib.sha256(sample.read_bytes()).hexdigest()
    parts = []
    for entry in manifest["parts"]:
        path = out / entry["file"]
        parts.append(path.read_bytes())
        receipt = json.loads(path.with_name(path.name + ".receipt.json").read_text())
        assert (
            hashlib.sha256(parts[-1]).hexdigest()
            == entry["source_sha256"]
            == receipt["source_sha256"]
        )
        assert receipt["requested_max_games"] == entry["games"] == 1
        assert receipt["parent_source_sha256"] == manifest["parent_source_sha256"]
    assert b"\n".join(parts) == sample.read_bytes()
    sample.write_bytes(sample.read_bytes() + b"\n")
    new_out = tmp_path / "stale"
    command[command.index("--output") + 1] = str(new_out)
    failure = subprocess.run(command, capture_output=True)
    assert failure.returncode != 0 and not new_out.exists()
