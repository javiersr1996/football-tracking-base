# Copied from tracklab@686508a: pipeline/src/tracklab/ingest/lfs.py
# Changes: header only (no code changes).
"""Git LFS pointer files: detection and parsing.

A pointer is the ~130-byte text Git stores in place of a large file. It is what
raw.githubusercontent.com serves for an LFS path; the real bytes live behind
media.githubusercontent.com. The pointer already carries the sha256 (``oid``)
and ``size`` of the real file, so freezing an LFS file costs one tiny request.
"""

from __future__ import annotations

from dataclasses import dataclass

LFS_HEADER = b"version https://git-lfs.github.com/spec/v1"
# Pointers are three short lines. Anything larger is real content.
MAX_POINTER_BYTES = 1024


@dataclass(frozen=True)
class LfsPointer:
    oid: str  # sha256 hex digest of the real file
    size: int  # size in bytes of the real file


def parse_pointer(data: bytes) -> LfsPointer | None:
    """Return the pointer described by ``data``, or None if it is not one."""
    if len(data) > MAX_POINTER_BYTES or not data.startswith(LFS_HEADER):
        return None
    fields: dict[str, str] = {}
    for line in data.decode("ascii", errors="replace").splitlines():
        key, _, value = line.partition(" ")
        fields[key] = value.strip()
    oid = fields.get("oid", "").removeprefix("sha256:")
    size = fields.get("size", "")
    if len(oid) != 64 or not size.isdigit():
        return None
    return LfsPointer(oid=oid, size=int(size))
