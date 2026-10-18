# Rewritten from tracklab@686508a: pipeline/src/tracklab/ingest/fetch.py
# Changes: removed freeze, the GitHub tree API and everything HuggingFace; User-Agent renamed;
# added --match (select_match) and the match_id/manifest_files fields of Receipt.
"""HTTP side of ingest: streaming downloads hashed on the fly and the driver
behind ``ftbase ingest``.

No ``git``, no ``git-lfs``, no clone. Small files come from the raw host, LFS
files from the media host; both are plain GET requests.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from ftbase.ingest.manifest import FileEntry, Source
from ftbase.ingest.select import select_match
from ftbase.ingest.verify import CHUNK_BYTES, sha256_file

RECEIPT_NAME = "_receipt.json"
ATTEMPTS = 3  # per file, on transport errors only

Log = Callable[[str], None]


class IntegrityError(Exception):
    """Downloaded bytes do not match the manifest."""


def _keep_token_on_github(request: httpx.Request) -> None:
    """GITHUB_TOKEN goes to GitHub hosts only, never to a host a redirect sends us to."""
    if not request.url.host.endswith("github.com") and not request.url.host.endswith(
        "githubusercontent.com"
    ):
        request.headers.pop("Authorization", None)


def make_client(transport: httpx.BaseTransport | None = None) -> httpx.Client:
    headers = {"User-Agent": "ftbase-ingest"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(
        transport=transport,
        headers=headers,
        follow_redirects=True,
        timeout=httpx.Timeout(60.0),
        event_hooks={"request": [_keep_token_on_github]},
    )


def download_verified(client: httpx.Client, url: str, dest: Path, entry: FileEntry) -> None:
    """Stream ``url`` into ``dest``, hashing on the fly. ``dest`` appears only if it matches."""
    part = dest.with_name(dest.name + ".part")
    dest.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    try:
        with client.stream("GET", url) as response, part.open("wb") as out:
            response.raise_for_status()
            for chunk in response.iter_bytes(CHUNK_BYTES):
                digest.update(chunk)
                out.write(chunk)
                size += len(chunk)
        if size != entry.bytes or digest.hexdigest() != entry.sha256:
            raise IntegrityError(
                f"got {size} bytes sha256={digest.hexdigest()}, "
                f"expected {entry.bytes} bytes sha256={entry.sha256}"
            )
        os.replace(part, dest)
    finally:
        part.unlink(missing_ok=True)


def _download_with_retry(
    client: httpx.Client, url: str, dest: Path, entry: FileEntry, log: Log
) -> None:
    for attempt in range(1, ATTEMPTS + 1):
        try:
            download_verified(client, url, dest, entry)
            return
        except httpx.TransportError as exc:
            if attempt == ATTEMPTS:
                raise
            log(f"retry {attempt}/{ATTEMPTS - 1}: {entry.path}: {exc}")


# --- ingest ------------------------------------------------------------------------


@dataclass
class Receipt:
    source_id: str
    commit: str
    verified: int
    downloaded: int
    failed: list[str]
    finished_at: str
    source_path: str | None = None  # set by ingest_local: where --from-existing verified in place
    match_id: int | None = None  # set by --match: only that match (plus matches.json) was ingested
    manifest_files: int = 0  # files in the full manifest, so a partial run says what it left out

    @property
    def summary(self) -> str:
        line = f"downloaded={self.downloaded} verified={self.verified} failed={len(self.failed)}"
        if self.match_id is not None:
            line += f" PARTIAL {self.verified}/{self.manifest_files}"
        return line


def ingest_source(
    client: httpx.Client,
    source: Source,
    raw_root: Path,
    log: Log = print,
    match_id: int | None = None,
) -> Receipt:
    """Make ``raw_root`` hold every manifest file with the right sha256.

    Per file: present and matching, skip; present and different, delete and
    download; absent, download. A failed file is recorded in the receipt and the
    run carries on, so a re-run resumes where this one left off. With ``match_id``
    only that match and ``matches.json`` are ingested and the receipt says so.
    """
    if source.commit is None or not source.files:
        raise RuntimeError(f"{source.source_id} has no pinned commit or no files")
    commit = source.commit
    manifest_files = len(source.files)
    if match_id is not None:
        source = select_match(source, match_id)
    verified = downloaded = 0
    failed: list[str] = []
    for entry in source.files:
        dest = raw_root / entry.path
        if dest.is_file():
            if sha256_file(dest) == entry.sha256:
                verified += 1
                continue
            log(f"mismatch: {entry.path} (sha256 differs, downloading again)")
            dest.unlink()
        url = source.lfs_url_for(entry.path) if entry.lfs else source.raw_url_for(entry.path)
        try:
            _download_with_retry(client, url, dest, entry, log)
        except (httpx.HTTPError, IntegrityError) as exc:
            log(f"failed: {entry.path}: {exc}")
            failed.append(entry.path)
            continue
        log(f"downloaded: {entry.path} ({entry.bytes} bytes)")
        downloaded += 1
        verified += 1
    receipt = Receipt(
        source_id=source.source_id,
        commit=commit,
        verified=verified,
        downloaded=downloaded,
        failed=failed,
        finished_at=datetime.now(UTC).isoformat(timespec="seconds"),
        match_id=match_id,
        manifest_files=manifest_files,
    )
    raw_root.mkdir(parents=True, exist_ok=True)
    (raw_root / RECEIPT_NAME).write_text(
        json.dumps(asdict(receipt), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return receipt
