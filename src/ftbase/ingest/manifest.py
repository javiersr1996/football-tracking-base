"""The sources manifest (``manifest/sources.json``).

It is versioned and lists, per source, the pinned commit and the sha256 of every
file the pipeline ingests. ``tracklab freeze`` fills ``commit`` and ``files``;
``tracklab ingest`` only reads it. Its shape is documented in tasks/todo.md,
section "Manifiesto de ingesta".
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

# Which files under data/ belong to a bundle, and the role each one plays.
# A path that matches nothing is skipped by freeze and reported, never dropped
# in silence. First match wins.
_ROLE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"^data/matches/(\d+)/\1_tracking_extrapolated\.jsonl$"), "tracking"),
    (re.compile(r"^data/matches/(\d+)/\1_match\.json$"), "match"),
    (re.compile(r"^data/matches/(\d+)/\1_dynamic_events\.csv$"), "events"),
    (re.compile(r"^data/matches/(\d+)/\1_phases_of_play\.csv$"), "phases"),
    (re.compile(r"^data/aggregates/[^/]+\.csv$"), "aggregate"),
    (re.compile(r"^data/matches\.json$"), "matches"),
    (re.compile(r"^data/bodypose/[^/]+\.jsonl\.gz$"), "bodypose_sample"),
    # The full-match body pose on HuggingFace (opendata-bodypose): one zip per match.
    (re.compile(r"^raw/(\d+)\.jsonl\.zip$"), "bodypose_zip"),
    # Basketball (opendata-basketball): same bundle shape, different filenames.
    (re.compile(r"^data/matches/(\d+)/\1_tracking_data\.jsonl\.gz$"), "tracking"),
    (re.compile(r"^data/matches/(\d+)/\1_game_data\.json$"), "game_data"),
    (re.compile(r"^data/matches/(\d+)/\1_dynamic_events\.json$"), "events"),
    (re.compile(r"^data/player_id_aliases\.csv$"), "player_aliases"),
)
_MATCH_DIR = re.compile(r"^data/matches/(\d+)/")
_MATCH_ZIP = re.compile(r"^raw/(\d+)\.jsonl\.zip$")


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
            raise RuntimeError(f"{self.source_id} is not frozen; run `tracklab freeze` first")
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

    def replace(self, source: Source) -> None:
        self.sources = [source if s.source_id == source.source_id else s for s in self.sources]


def role_for(path: str) -> str | None:
    """Role of a repository path inside the bundle, or None if it is not part of it."""
    for pattern, role in _ROLE_PATTERNS:
        if pattern.match(path):
            return role
    return None


def match_id_for(path: str) -> int | None:
    """Match id for paths under data/matches/{id}/ and for raw/{id}.jsonl.zip, else None."""
    found = _MATCH_DIR.match(path) or _MATCH_ZIP.match(path)
    return int(found.group(1)) if found else None


def load_manifest(path: Path) -> Manifest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"{path}: expected schema_version {SCHEMA_VERSION}")
    return Manifest(
        schema_version=SCHEMA_VERSION,
        sources=[_source_from(entry) for entry in raw["sources"]],
    )


def save_manifest(manifest: Manifest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(manifest), indent=2) + "\n", encoding="utf-8", newline="\n")


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
