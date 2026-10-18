# Copied from tracklab@686508a: pipeline/src/tracklab/ingest/manifest.py
# Changes: trimmed to the football source (no file-role patterns, role_for, match_id_for,
# save_manifest or Manifest.replace: they served `freeze` and the other sports).
"""The sources manifest (``manifest/sources.json``).

It is versioned and lists, per source, the pinned commit and the sha256 of every
file the pipeline ingests. ``ftbase ingest`` only reads it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


@dataclass
class FileEntry:
    path: str
    bytes: int
    sha256: str
    lfs: bool
    role: str
    match_id: int | None


@dataclass
class Source:
    source_id: str
    sport: str
    kind: str
    repo: str
    ref: str
    commit: str | None
    license: str
    raw_url: str
    lfs_url: str
    files: list[FileEntry]

    def raw_url_for(self, path: str) -> str:
        return self._base(self.raw_url) + path

    def lfs_url_for(self, path: str) -> str:
        return self._base(self.lfs_url) + path

    def _base(self, template: str) -> str:
        if self.commit is None:
            raise RuntimeError(f"{self.source_id} has no pinned commit")
        return template.format(commit=self.commit)


@dataclass
class Manifest:
    schema_version: int
    sources: list[Source]

    def source(self, source_id: str) -> Source:
        for source in self.sources:
            if source.source_id == source_id:
                return source
        known = ", ".join(s.source_id for s in self.sources)
        raise KeyError(f"unknown source '{source_id}' (known: {known})")


def load_manifest(path: Path) -> Manifest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"{path}: expected schema_version {SCHEMA_VERSION}")
    return Manifest(
        schema_version=SCHEMA_VERSION,
        sources=[_source_from(entry) for entry in raw["sources"]],
    )


def _source_from(entry: dict[str, Any]) -> Source:
    return Source(
        source_id=entry["source_id"],
        sport=entry["sport"],
        kind=entry["kind"],
        repo=entry["repo"],
        ref=entry["ref"],
        commit=entry["commit"],
        license=entry["license"],
        raw_url=entry["raw_url"],
        lfs_url=entry["lfs_url"],
        files=[FileEntry(**f) for f in entry["files"]],
    )
