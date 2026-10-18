"""HTTP side of ingest: GitHub lookups, streaming downloads hashed on the fly,
and the two drivers behind ``tracklab freeze`` and ``tracklab ingest``.

No ``git``, no ``git-lfs``, no clone. Small files come from the raw host, LFS
files from the media host; both are plain GET requests.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

import httpx

from tracklab.ingest.lfs import MAX_POINTER_BYTES, parse_pointer
from tracklab.ingest.manifest import FileEntry, Source, match_id_for, role_for
from tracklab.ingest.verify import CHUNK_BYTES, sha256_file

API_URL = "https://api.github.com"
API_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
HF_API_URL = "https://huggingface.co/api/datasets"
RECEIPT_NAME = "_receipt.json"
ATTEMPTS = 3  # per file, on transport errors only

Log = Callable[[str], None]


class IntegrityError(Exception):
    """Downloaded bytes do not match the manifest."""


def _keep_token_on_github(request: httpx.Request) -> None:
    """GITHUB_TOKEN goes to GitHub hosts only; HuggingFace is public and gets no credential."""
    if not request.url.host.endswith("github.com") and not request.url.host.endswith(
        "githubusercontent.com"
    ):
        request.headers.pop("Authorization", None)


def make_client(transport: httpx.BaseTransport | None = None) -> httpx.Client:
    headers = {"User-Agent": "tracklab-ingest"}
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


# --- GitHub API -----------------------------------------------------------------


@dataclass(frozen=True)
class TreeEntry:
    path: str
    size: int


def resolve_commit(client: httpx.Client, repo: str, ref: str) -> str:
    response = client.get(f"{API_URL}/repos/{repo}/commits/{ref}", headers=API_HEADERS)
    response.raise_for_status()
    return str(response.json()["sha"])


def list_tree(client: httpx.Client, repo: str, commit: str) -> list[TreeEntry]:
    response = client.get(
        f"{API_URL}/repos/{repo}/git/trees/{commit}",
        params={"recursive": "1"},
        headers=API_HEADERS,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("truncated"):
        raise RuntimeError(f"GitHub truncated the tree of {repo}@{commit}; refusing to freeze it")
    return [TreeEntry(e["path"], int(e["size"])) for e in body["tree"] if e["type"] == "blob"]


@dataclass(frozen=True)
class HfEntry:
    path: str
    size: int
    sha256: str | None  # the LFS oid; None for a file stored in git proper


def resolve_hf_revision(client: httpx.Client, repo: str, ref: str) -> str:
    response = client.get(f"{HF_API_URL}/{repo}/revision/{ref}")
    response.raise_for_status()
    return str(response.json()["sha"])


def list_hf_tree(client: httpx.Client, repo: str, revision: str) -> list[HfEntry]:
    """Every file of the revision with its size and LFS oid (= sha256), without fetching a byte."""
    entries: list[HfEntry] = []
    url: str | None = f"{HF_API_URL}/{repo}/tree/{revision}?recursive=1"
    while url is not None:
        response = client.get(url)
        response.raise_for_status()
        for e in response.json():
            if e["type"] == "file":
                lfs = e.get("lfs")
                entries.append(HfEntry(e["path"], int(e["size"]), lfs["oid"] if lfs else None))
        url = response.links.get("next", {}).get("url")
    return entries


# --- Streaming downloads ------------------------------------------------------------


@dataclass(frozen=True)
class StreamDigest:
    sha256: str
    size: int
    head: bytes  # first bytes, enough to recognise an LFS pointer


def stream_sha256(client: httpx.Client, url: str) -> StreamDigest:
    """GET ``url`` and hash the body as it streams. Only the head is kept."""
    digest = hashlib.sha256()
    size = 0
    head = b""
    with client.stream("GET", url) as response:
        response.raise_for_status()
        for chunk in response.iter_bytes(CHUNK_BYTES):
            if len(head) < MAX_POINTER_BYTES:
                head += chunk[: MAX_POINTER_BYTES - len(head)]
            digest.update(chunk)
            size += len(chunk)
    return StreamDigest(digest.hexdigest(), size, head)


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


# --- freeze ------------------------------------------------------------------------


def freeze_source(client: httpx.Client, source: Source, log: Log = print) -> Source:
    """Pin ``source.ref`` to a commit and record bytes and sha256 of every bundle file.

    LFS files are recognised by their pointer and cost one tiny request; the rest
    are streamed once to compute their sha256. Nothing is written to ``data/``.
    """
    if source.kind == "huggingface":
        return freeze_hf(client, source, log)
    commit = resolve_commit(client, source.repo, source.ref)
    frozen = replace(source, commit=commit, files=[])
    files: list[FileEntry] = []
    for entry in list_tree(client, source.repo, commit):
        if not entry.path.startswith("data/"):
            continue
        role = role_for(entry.path)
        if role is None:
            log(f"skipped: {entry.path} (no role in the bundle)")
            continue
        digest = stream_sha256(client, frozen.raw_url_for(entry.path))
        pointer = parse_pointer(digest.head) if digest.size <= MAX_POINTER_BYTES else None
        match_id = match_id_for(entry.path)
        if pointer is not None:
            files.append(FileEntry(entry.path, pointer.size, pointer.oid, True, role, match_id))
        else:
            files.append(FileEntry(entry.path, digest.size, digest.sha256, False, role, match_id))
    frozen.files = sorted(files, key=lambda f: f.path)
    return frozen


def freeze_hf(client: httpx.Client, source: Source, log: Log = print) -> Source:
    """Pin a HuggingFace dataset to its revision. Sizes and sha256 come from the tree
    API (the LFS oid is the sha256); a file with a role but no LFS oid is streamed once."""
    commit = resolve_hf_revision(client, source.repo, source.ref)
    frozen = replace(source, commit=commit, files=[])
    files: list[FileEntry] = []
    for entry in list_hf_tree(client, source.repo, commit):
        role = role_for(entry.path)
        if role is None:
            log(f"skipped: {entry.path} (no role in the bundle)")
            continue
        match_id = match_id_for(entry.path)
        if entry.sha256 is not None:
            files.append(FileEntry(entry.path, entry.size, entry.sha256, True, role, match_id))
        else:
            digest = stream_sha256(client, frozen.raw_url_for(entry.path))
            files.append(FileEntry(entry.path, digest.size, digest.sha256, False, role, match_id))
    frozen.files = sorted(files, key=lambda f: f.path)
    return frozen


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

    @property
    def summary(self) -> str:
        return f"downloaded={self.downloaded} verified={self.verified} failed={len(self.failed)}"


def ingest_source(
    client: httpx.Client, source: Source, raw_root: Path, log: Log = print
) -> Receipt:
    """Make ``raw_root`` hold every manifest file with the right sha256.

    Per file: present and matching, skip; present and different, delete and
    download; absent, download. A failed file is recorded in the receipt and the
    run carries on, so a re-run resumes where this one left off.
    """
    if source.commit is None or not source.files:
        raise RuntimeError(
            f"{source.source_id} is not frozen; run `tracklab freeze {source.source_id}` first"
        )
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
        commit=source.commit,
        verified=verified,
        downloaded=downloaded,
        failed=failed,
        finished_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    raw_root.mkdir(parents=True, exist_ok=True)
    (raw_root / RECEIPT_NAME).write_text(
        json.dumps(asdict(receipt), indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return receipt
