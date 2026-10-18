"""Local-clone side of ingest: freeze and verify a source straight out of a
git working copy already on disk, with no network and no ``git-lfs``.

Used for opendata-basketball, whose raw bytes already live in a git clone
outside this repo. A git blob for an LFS-tracked path is always the pointer
text (the smudge filter that expands it into real bytes only touches the
worktree, never the object database), so ``git cat-file -p HEAD:<path>`` gives
the same ``oid``/``size`` a raw LFS pointer download would, for the cost of a
local process instead of a request. Everything else is hashed straight from
the same command's output.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path

from tracklab.ingest.fetch import RECEIPT_NAME, Log, Receipt
from tracklab.ingest.lfs import MAX_POINTER_BYTES, parse_pointer
from tracklab.ingest.manifest import FileEntry, Source, match_id_for, role_for
from tracklab.ingest.verify import only_line_endings_differ, verify_file


def _git(repo_dir: Path, *args: str) -> bytes:
    result = subprocess.run(["git", *args], cwd=repo_dir, capture_output=True, check=True)
    return result.stdout


def freeze_local(repo_dir: Path, source: Source, log: Log = print) -> Source:
    """Freeze ``source`` by reading its files out of ``repo_dir`` at HEAD.

    Mirrors ``fetch.freeze_source`` file for file, but the tree listing and
    every file's bytes come from local git objects instead of the GitHub API
    and raw host.
    """
    commit = _git(repo_dir, "rev-parse", "HEAD").decode("ascii").strip()
    paths = _git(repo_dir, "ls-tree", "-r", "--name-only", "HEAD").decode("utf-8").splitlines()
    files: list[FileEntry] = []
    for path in paths:
        if not path.startswith("data/"):
            continue
        role = role_for(path)
        if role is None:
            log(f"skipped: {path} (no role in the bundle)")
            continue
        content = _git(repo_dir, "cat-file", "-p", f"HEAD:{path}")
        pointer = parse_pointer(content) if len(content) <= MAX_POINTER_BYTES else None
        match_id = match_id_for(path)
        if pointer is not None:
            files.append(FileEntry(path, pointer.size, pointer.oid, True, role, match_id))
        else:
            digest = hashlib.sha256(content).hexdigest()
            files.append(FileEntry(path, len(content), digest, False, role, match_id))
    return replace(source, commit=commit, files=sorted(files, key=lambda f: f.path))


def ingest_local(
    source: Source, existing_root: Path, receipt_root: Path, log: Log = print
) -> Receipt:
    """Verify every manifest file inside ``existing_root`` in place.

    No download, no copy: a file either already hashes to what the manifest
    says or it is reported failed. The receipt still lands under
    ``receipt_root`` (the usual ``data/raw/<source_id>/`` bookkeeping spot),
    carrying ``existing_root`` as ``source_path`` since the bytes themselves
    stay where they were found.
    """
    if source.commit is None or not source.files:
        raise RuntimeError(
            f"{source.source_id} is not frozen; run `tracklab freeze {source.source_id}` first"
        )
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
        commit=source.commit,
        verified=verified,
        downloaded=0,
        failed=failed,
        finished_at=datetime.now(UTC).isoformat(timespec="seconds"),
        source_path=str(existing_root),
    )
    receipt_root.mkdir(parents=True, exist_ok=True)
    (receipt_root / RECEIPT_NAME).write_text(
        json.dumps(asdict(receipt), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return receipt
