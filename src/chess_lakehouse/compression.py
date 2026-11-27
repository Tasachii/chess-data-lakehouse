"""Zstandard readers that distinguish complete frames from truncated EOF."""

from __future__ import annotations

import codecs
from collections.abc import Iterator
from typing import BinaryIO

import zstandard


def decompressed_chunks(source: BinaryIO) -> Iterator[bytes]:
    """Stream one or more frames, raising if physical EOF interrupts a frame.

    Consumers may deliberately stop early after a bounded prefix, without claiming
    that the unread archive suffix is intact.
    """
    frame = None
    completed = False
    while chunk := source.read(64 * 1024):
        while chunk:
            if frame is None:
                frame = zstandard.ZstdDecompressor().decompressobj()
            decoded = frame.decompress(chunk)
            if decoded:
                yield decoded
            if frame.eof:
                completed = True
                chunk = frame.unused_data
                frame = None
            else:
                break
    if frame is not None or not completed:
        raise ValueError("Incomplete or empty Zstandard frame at end of input")


def decoded_lines(source: BinaryIO) -> Iterator[str]:
    """Incrementally decode UTF-8 and emit full lines without reading the archive."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    pending = ""
    for chunk in decompressed_chunks(source):
        lines = (pending + decoder.decode(chunk)).split("\n")
        pending = lines.pop()
        yield from lines
    pending += decoder.decode(b"", final=True)
    if pending:
        yield pending
