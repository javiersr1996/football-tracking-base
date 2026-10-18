"""freeze and ingest against a fake GitHub served by httpx.MockTransport. No network.

Covers the acceptance paths of task 2: fresh ingest, idempotent second pass,
mutation of one file (re-downloaded), corruption with the network down (exit 1,
listed in failed), plus a server that serves bytes that do not hash as promised.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from tracklab.cli import main
from tracklab.ingest.fetch import RECEIPT_NAME, freeze_source, ingest_source
from tracklab.ingest.lfs import LfsPointer, parse_pointer
from tracklab.ingest.manifest import Manifest, Source, load_manifest, save_manifest
from tracklab.ingest.verify import CHUNK_BYTES, sha256_file

REPO = "Acme/opendata"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
SOURCE_ID = "opendata-football"
MATCH_JSON = "data/matches/1996435/1996435_match.json"
TRACKING = "data/matches/1996435/1996435_tracking_extrapolated.jsonl"

# Small files served verbatim by the raw host. Two of them must be ignored by freeze:
# README.md sits outside data/ and data/bodypose/README.md has no role in the bundle.
RAW_FILES: dict[str, bytes] = {
    "data/matches.json": b'[{"id": 1996435}]\n',
    MATCH_JSON: b'{"id": 1996435, "home_team": {"short_name": "Sydney FC"}}\n',
    "data/matches/1996435/1996435_dynamic_events.csv": b"frame,x,y\n1,0.5,0.5\n",
    "data/matches/1996435/1996435_phases_of_play.csv": b"phase_index,start\n1,0\n",
    "data/aggregates/aus1league_physicalaggregates_20242025.csv": b"player_id,distance\n",
    "data/bodypose/sample_1925299_phase406.jsonl.gz": b"\x1f\x8b\x08\x00fake",
    "data/bodypose/README.md": b"# not part of the bundle\n",
    "README.md": b"# outside data/\n",
}
# Large file behind LFS: raw serves the pointer, media serves the bytes. Bigger than
# two chunks so the streaming hash is exercised across chunk boundaries.
TRACKING_BLOB = os.urandom(2 * CHUNK_BYTES + 17)
BUNDLE_SIZE = 7  # everything under data/ with a role, including the LFS file


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pointer_for(blob: bytes) -> bytes:
    return (
        f"version https://git-lfs.github.com/spec/v1\noid sha256:{sha256(blob)}\nsize {len(blob)}\n"
    ).encode()


class FakeGitHub:
    """api.github.com, raw.githubusercontent.com and media.githubusercontent.com in one handler."""

    def __init__(self) -> None:
        self.raw = dict(RAW_FILES)
        self.raw[TRACKING] = pointer_for(TRACKING_BLOB)
        self.media = {TRACKING: TRACKING_BLOB}
        self.hits: Counter[str] = Counter()
        self.offline = False

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.offline:
            raise httpx.ConnectError("network is unreachable", request=request)
        url = request.url
        self.hits[url.host] += 1
        if url.host == "api.github.com":
            if url.path == f"/repos/{REPO}/commits/master":
                return httpx.Response(200, json={"sha": COMMIT})
            if url.path == f"/repos/{REPO}/git/trees/{COMMIT}":
                tree = [{"path": "data", "type": "tree"}] + [
                    {"path": path, "type": "blob", "size": len(content)}
                    for path, content in self.raw.items()
                ]
                return httpx.Response(200, json={"sha": COMMIT, "truncated": False, "tree": tree})
        if url.host == "raw.githubusercontent.com":
            path = url.path.removeprefix(f"/{REPO}/{COMMIT}/")
            if path in self.raw:
                return httpx.Response(200, content=self.raw[path])
        if url.host == "media.githubusercontent.com":
            path = url.path.removeprefix(f"/media/{REPO}/{COMMIT}/")
            if path in self.media:
                return httpx.Response(200, content=self.media[path])
        return httpx.Response(404, text=f"not found: {url}")


def unfrozen_source() -> Source:
    return Source(
        source_id=SOURCE_ID,
        sport="football",
        kind="github",
        repo=REPO,
        ref="master",
        commit=None,
        license="MIT",
        raw_url=f"https://raw.githubusercontent.com/{REPO}/{{commit}}/",
        lfs_url=f"https://media.githubusercontent.com/media/{REPO}/{{commit}}/",
        files=[],
    )


@pytest.fixture
def github() -> FakeGitHub:
    return FakeGitHub()


@pytest.fixture
def manifest_path(tmp_path: Path, github: FakeGitHub) -> Path:
    """A frozen manifest on disk, as `tracklab freeze` would leave it."""
    with github.client() as client:
        frozen = freeze_source(client, unfrozen_source(), log=lambda _: None)
    path = tmp_path / "manifest" / "sources.json"
    save_manifest(Manifest(schema_version=1, sources=[frozen]), path)
    github.hits.clear()
    return path


@pytest.fixture
def raw_root(tmp_path: Path) -> Path:
    return tmp_path / "data" / "raw" / SOURCE_ID


@pytest.fixture
def cli_client(github: FakeGitHub, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr("tracklab.cli.make_client", github.client)
    yield


def run_cli(manifest_path: Path, tmp_path: Path) -> int:
    return main(
        [
            "ingest",
            SOURCE_ID,
            "--manifest",
            str(manifest_path),
            "--data-dir",
            str(tmp_path / "data"),
        ]
    )


# --- LFS pointers ---------------------------------------------------------------------


def test_parse_pointer_reads_oid_and_size() -> None:
    assert parse_pointer(pointer_for(b"abc")) == LfsPointer(oid=sha256(b"abc"), size=3)


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b'{"frame": 0}',
        b"version https://git-lfs.github.com/spec/v1\noid sha256:short\nsize 1\n",
    ],
)
def test_parse_pointer_rejects_non_pointers(data: bytes) -> None:
    assert parse_pointer(data) is None


# --- freeze ---------------------------------------------------------------------------


def test_freeze_pins_commit_and_hashes(github: FakeGitHub) -> None:
    logs: list[str] = []
    with github.client() as client:
        frozen = freeze_source(client, unfrozen_source(), log=logs.append)

    assert frozen.commit == COMMIT
    assert len(frozen.files) == BUNDLE_SIZE
    assert logs == ["skipped: data/bodypose/README.md (no role in the bundle)"]
    assert github.hits["media.githubusercontent.com"] == 0, "freeze must not fetch LFS bodies"

    by_path = {f.path: f for f in frozen.files}
    tracking = by_path[TRACKING]
    assert (tracking.lfs, tracking.bytes, tracking.sha256, tracking.role, tracking.match_id) == (
        True,
        len(TRACKING_BLOB),
        sha256(TRACKING_BLOB),
        "tracking",
        1996435,
    )
    match = by_path[MATCH_JSON]
    assert (match.lfs, match.bytes, match.sha256, match.role, match.match_id) == (
        False,
        len(RAW_FILES[MATCH_JSON]),
        sha256(RAW_FILES[MATCH_JSON]),
        "match",
        1996435,
    )
    assert {f.role: f.match_id for f in frozen.files if f.match_id is None} == {
        "matches": None,
        "aggregate": None,
        "bodypose_sample": None,
    }


def test_freeze_cli_writes_manifest(
    tmp_path: Path, cli_client: None, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "sources.json"
    save_manifest(Manifest(schema_version=1, sources=[unfrozen_source()]), path)

    assert main(["freeze", SOURCE_ID, "--manifest", str(path)]) == 0

    assert capsys.readouterr().out.splitlines()[-1] == f"commit={COMMIT} files={BUNDLE_SIZE} lfs=1"
    saved = load_manifest(path).source(SOURCE_ID)
    assert saved.commit == COMMIT
    assert [f.path for f in saved.files] == sorted(f.path for f in saved.files)


# --- ingest: the three acceptance paths -------------------------------------------------


def test_fresh_ingest_then_idempotent_second_pass(
    github: FakeGitHub,
    manifest_path: Path,
    raw_root: Path,
    tmp_path: Path,
    cli_client: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert run_cli(manifest_path, tmp_path) == 0
    assert capsys.readouterr().out.splitlines()[-1] == "downloaded=7 verified=7 failed=0"
    receipt = json.loads((raw_root / RECEIPT_NAME).read_text())
    assert receipt["failed"] == []
    assert (receipt["commit"], receipt["verified"], receipt["downloaded"]) == (COMMIT, 7, 7)
    assert (raw_root / TRACKING).read_bytes() == TRACKING_BLOB
    assert (raw_root / MATCH_JSON).read_bytes() == RAW_FILES[MATCH_JSON]
    assert not (raw_root / "data" / "bodypose" / "README.md").exists()
    assert not list(raw_root.rglob("*.part"))

    github.hits.clear()
    assert run_cli(manifest_path, tmp_path) == 0
    assert capsys.readouterr().out.splitlines()[-1] == "downloaded=0 verified=7 failed=0"
    assert sum(github.hits.values()) == 0, "second pass must not touch the network"


def test_mutated_file_is_named_and_downloaded_again(
    github: FakeGitHub,
    manifest_path: Path,
    raw_root: Path,
    tmp_path: Path,
    cli_client: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_cli(manifest_path, tmp_path)
    capsys.readouterr()
    target = raw_root / MATCH_JSON
    target.write_bytes(target.read_bytes()[:-1])  # truncate one byte
    github.hits.clear()

    assert run_cli(manifest_path, tmp_path) == 0

    out = capsys.readouterr().out
    assert f"mismatch: {MATCH_JSON}" in out
    assert out.splitlines()[-1] == "downloaded=1 verified=7 failed=0"
    assert target.read_bytes() == RAW_FILES[MATCH_JSON]
    assert github.hits["raw.githubusercontent.com"] == 1
    assert github.hits["media.githubusercontent.com"] == 0


def test_corrupt_file_offline_exits_1_and_lands_in_failed(
    github: FakeGitHub,
    manifest_path: Path,
    raw_root: Path,
    tmp_path: Path,
    cli_client: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_cli(manifest_path, tmp_path)
    capsys.readouterr()
    target = raw_root / TRACKING
    data = bytearray(target.read_bytes())
    data[1000] ^= 0xFF  # flip one byte, size unchanged
    target.write_bytes(data)
    github.offline = True

    assert run_cli(manifest_path, tmp_path) == 1

    out = capsys.readouterr().out
    assert f"failed: {TRACKING}" in out
    assert out.splitlines()[-1] == "downloaded=0 verified=6 failed=1"
    receipt = json.loads((raw_root / RECEIPT_NAME).read_text())
    assert receipt["failed"] == [TRACKING]
    assert not target.exists(), "a corrupt file must not survive as if it were good"
    assert not list(raw_root.rglob("*.part"))


def test_server_serving_wrong_bytes_is_rejected(
    github: FakeGitHub, manifest_path: Path, raw_root: Path
) -> None:
    """The hash check has teeth: bytes that do not match the manifest never land."""
    github.media[TRACKING] = TRACKING_BLOB[:-1] + bytes([TRACKING_BLOB[-1] ^ 0x01])
    source = load_manifest(manifest_path).source(SOURCE_ID)
    logs: list[str] = []

    with github.client() as client:
        receipt = ingest_source(client, source, raw_root, log=logs.append)

    assert receipt.failed == [TRACKING]
    assert (receipt.downloaded, receipt.verified) == (6, 6)
    assert any(line.startswith(f"failed: {TRACKING}") for line in logs)
    assert not (raw_root / TRACKING).exists()
    assert not list(raw_root.rglob("*.part"))
    others = [f for f in source.files if f.path != TRACKING]
    assert all(sha256_file(raw_root / f.path) == f.sha256 for f in others)


def test_ingest_refuses_unfrozen_source(tmp_path: Path, github: FakeGitHub) -> None:
    with github.client() as client, pytest.raises(RuntimeError, match="not frozen"):
        ingest_source(client, unfrozen_source(), tmp_path, log=lambda _: None)
