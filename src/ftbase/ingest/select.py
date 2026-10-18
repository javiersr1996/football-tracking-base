# New in ftbase (no tracklab origin): the `--match` fast path.
"""Subset of a source for ``ftbase ingest --match <id>``.

One match is four files (match.json, tracking, dynamic events, phases of play) plus
``matches.json``, the index every reader of a single match needs. That is a small
fraction of the source, so a reviewer can reproduce one match before downloading all of them.
"""

from __future__ import annotations

from dataclasses import replace

from ftbase.ingest.manifest import Source

MATCH_INDEX_ROLE = "matches"


def select_match(source: Source, match_id: int) -> Source:
    """``source`` restricted to the files of ``match_id`` plus the match index."""
    files = [f for f in source.files if f.match_id == match_id or f.role == MATCH_INDEX_ROLE]
    if not any(f.match_id == match_id for f in files):
        known = sorted({f.match_id for f in source.files if f.match_id is not None})
        raise KeyError(f"unknown match {match_id} in {source.source_id} (known: {known})")
    return replace(source, files=files)
