# Copied from tracklab@686508a: pipeline/src/tracklab/ingest/verify.py
# Changes: header only (no code changes).
"""Streaming sha256 of files on disk.

Files are hashed in fixed-size chunks so a 100 MB tracking file never sits in
memory at once.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

CHUNK_BYTES = 1 << 20  # 1 MiB


def sha256_file(path: Path) -> str:
    """Hex sha256 of the file at ``path``, read in ``CHUNK_BYTES`` chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def verify_file(path: Path, sha256: str) -> bool:
    """True if ``path`` exists and hashes to ``sha256``."""
    return path.is_file() and sha256_file(path) == sha256


def only_line_endings_differ(path: Path, sha256: str) -> bool:
    """True if ``path`` hashes to ``sha256`` once its CRLF are turned back into LF:
    what a Windows checkout with ``core.autocrlf=true`` does to a text file."""
    if not path.is_file():
        return False
    data = path.read_bytes()
    return b"\r\n" in data and hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest() == sha256
