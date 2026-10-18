"""Writes a synthetic manifest for the bronze tests (ftbase itself never writes one)."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from ftbase.ingest.manifest import Manifest


def save_manifest(manifest: Manifest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(manifest), indent=2) + "\n", encoding="utf-8", newline="\n")
