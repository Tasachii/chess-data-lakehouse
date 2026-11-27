"""Deterministic batch identity and trustworthy download receipts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit


def receipt_path(source: Path) -> Path:
    return source.with_name(source.name + ".receipt.json")


def source_metadata(source_url: str) -> dict:
    url = urlsplit(source_url)
    if url.scheme == "fixture":
        kind, license_id, license_url = (
            "synthetic_fixture",
            "MIT",
            "https://opensource.org/license/mit",
        )
    elif url.scheme == "https" and url.netloc == "database.lichess.org":
        kind, license_id, license_url = (
            "lichess_public",
            "CC0-1.0",
            "https://creativecommons.org/publicdomain/zero/1.0/",
        )
    else:
        kind, license_id, license_url = "external", None, None
    return {"source_kind": kind, "source_license": license_id, "source_license_url": license_url}


def batch_id(source_url: str, source_sha256: str) -> str:
    return hashlib.sha256(f"{source_url}\n{source_sha256}".encode()).hexdigest()


def input_receipt(source: Path, source_url: str, source_sha256: str) -> dict:
    provenance = {
        **source_metadata(source_url),
        "source_url": source_url,
        "source_sha256": source_sha256,
        "batch_id": batch_id(source_url, source_sha256),
        "requested_max_games": None,
        "retrieved_at_utc": None,
    }
    path = receipt_path(source)
    if path.exists():
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if receipt.get("source_sha256") != source_sha256 or receipt.get("source_url") != source_url:
            raise ValueError(f"Download receipt does not match input bytes and URL: {path}")
        bound = receipt["requested_max_games"]
        retrieved_at = receipt["retrieved_at_utc"]
        if (
            type(bound) is not int
            or bound < 1
            or receipt.get("games_written") != bound
            or datetime.fromisoformat(retrieved_at).utcoffset() is None
        ):
            raise ValueError(f"Invalid download receipt metadata: {path}")
        provenance.update({"requested_max_games": bound, "retrieved_at_utc": retrieved_at})
    return provenance
