# Copied from tracklab@686508a: pipeline/src/tracklab/ingest/local.py
# Changes: removed freeze_local and the git plumbing it needed; added --match (select_match).
"""Local-clone side of ingest: verify a source in place inside a copy that is
already on disk (``ftbase ingest --from-existing``), with no network.

Used when a clone of SkillCorner/opendata already exists, for instance if the
provider's LFS quota is spent. The existing directory is only read; the receipt
is written under the data root.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from ftbase.ingest.fetch import RECEIPT_NAME, Log, Receipt
from ftbase.ingest.manifest import Source
from ftbase.ingest.select import select_match
from ftbase.ingest.verify import only_line_endings_differ, verify_file


def ingest_local(
    source: Source,
    existing_root: Path,
    receipt_root: Path,
    log: Log = print,
    match_id: int | None = None,
) -> Receipt:
    """Verify every manifest file inside ``existing_root`` in place.

    No download, no copy: a file either already hashes to what the manifest
    says or it is reported failed. The receipt still lands under
    ``receipt_root`` (the usual ``data/raw/<source_id>/`` bookkeeping spot),
    carrying ``existing_root`` as ``source_path`` since the bytes themselves
    stay where they were found. With ``match_id`` only that match and
    ``matches.json`` are checked and the receipt says so.
    """
    if source.commit is None or not source.files:
        raise RuntimeError(f"{source.source_id} has no pinned commit or no files")
    commit = source.commit
    manifest_files = len(source.files)
    if match_id is not None:
        source = select_match(source, match_id)
    verified = 0
    failed: list[str] = []
    for entry in source.files:
        if verify_file(existing_root / entry.path, entry.sha256):
            verified += 1
        elif only_line_endings_differ(existing_root / entry.path, entry.sha256):
            log(
                f"failed: {entry.path} (right bytes, CRLF line endings: this checkout "
                "converted them; set `git config core.autocrlf false` and check it out again)"
            )
            failed.append(entry.path)
        else:
            log(f"failed: {entry.path}")
            failed.append(entry.path)
    receipt = Receipt(
        source_id=source.source_id,
        commit=commit,
        verified=verified,
        downloaded=0,
        failed=failed,
        finished_at=datetime.now(UTC).isoformat(timespec="seconds"),
        source_path=existing_root.resolve().as_posix(),
        match_id=match_id,
        manifest_files=manifest_files,
    )
    receipt_root.mkdir(parents=True, exist_ok=True)
    (receipt_root / RECEIPT_NAME).write_text(
        json.dumps(asdict(receipt), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return receipt
