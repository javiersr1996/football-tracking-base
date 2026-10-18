# Copied from tracklab@686508a: pipeline/tests/unit/test_ingest.py
# Changes: football only (no basketball, HuggingFace or freeze tests); the manifest is built from
# the fake bundle instead of frozen; added tests for --match and --from-existing.
"""ingest against a fake GitHub served by httpx.MockTransport. No network.

Covers fresh ingest, idempotent second pass, mutation of one file (re-downloaded),
corruption with the network down (exit 1, listed in failed), a server that serves bytes
that do not hash as promised, the --match fast path and --from-existing verification.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path

import httpx
import pytest

from ftbase.cli import default_manifest, main
from ftbase.ingest.fetch import RECEIPT_NAME, ingest_source
from ftbase.ingest.lfs import LfsPointer, parse_pointer
from ftbase.ingest.local import ingest_local
from ftbase.ingest.manifest import FileEntry, Source, load_manifest
from ftbase.ingest.select import select_match
from ftbase.ingest.verify import CHUNK_BYTES, sha256_file

REPO = "Acme/opendata"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
SOURCE_ID = "opendata-football"
MATCH = 1996435
OTHER_MATCH = 2007448
MATCH_JSON = f"data/matches/{MATCH}/{MATCH}_match.json"
TRACKING = f"data/matches/{MATCH}/{MATCH}_tracking_extrapolated.jsonl"
OTHER_TRACKING = f"data/matches/{OTHER_MATCH}/{OTHER_MATCH}_tracking_extrapolated.jsonl"

# Small files served verbatim by the raw host.
RAW_FILES: dict[str, bytes] = {
    "data/matches.json": b'[{"id": 1996435}, {"id": 2007448}]\n',
    MATCH_JSON: b'{"id": 1996435, "home_team": {"short_name": "Sydney FC"}}\n',
    f"data/matches/{MATCH}/{MATCH}_dynamic_events.csv": b"frame,x,y\n1,0.5,0.5\n",
    f"data/matches/{MATCH}/{MATCH}_phases_of_play.csv": b"phase_index,start\n1,0\n",
    f"data/matches/{OTHER_MATCH}/{OTHER_MATCH}_match.json": b'{"id": 2007448}\n',
    f"data/matches/{OTHER_MATCH}/{OTHER_MATCH}_dynamic_events.csv": b"frame,x,y\n2,0.1,0.1\n",
    f"data/matches/{OTHER_MATCH}/{OTHER_MATCH}_phases_of_play.csv": b"phase_index,start\n2,0\n",
    "data/aggregates/aus1league_physicalaggregates_20242025.csv": b"player_id,distance\n",
    "data/bodypose/sample_1925299_phase406.jsonl.gz": b"\x1f\x8b\x08\x00fake",
}
# Large file behind LFS: raw serves the pointer, media serves the bytes. Bigger than
# two chunks so the streaming hash is exercised across chunk boundaries.
TRACKING_BLOB = os.urandom(2 * CHUNK_BYTES + 17)
OTHER_TRACKING_BLOB = os.urandom(1000)
MEDIA_FILES = {TRACKING: TRACKING_BLOB, OTHER_TRACKING: OTHER_TRACKING_BLOB}
BUNDLE_SIZE = 11  # 9 small files plus the 2 LFS tracking files
MATCH_SIZE = 5  # the 4 files of one match plus matches.json

ROLE_BY_SUFFIX = {
    "_match.json": "match",
    "_tracking_extrapolated.jsonl": "tracking",
    "_dynamic_events.csv": "events",
    "_phases_of_play.csv": "phases",
}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pointer_for(blob: bytes) -> bytes:
    return (
        f"version https://git-lfs.github.com/spec/v1\noid sha256:{sha256(blob)}\nsize {len(blob)}\n"
    ).encode()


class FakeGitHub:
    """raw.githubusercontent.com and media.githubusercontent.com in one handler."""

    def __init__(self) -> None:
        self.raw = dict(RAW_FILES)
        self.raw[TRACKING] = pointer_for(TRACKING_BLOB)
        self.raw[OTHER_TRACKING] = pointer_for(OTHER_TRACKING_BLOB)
        self.media = dict(MEDIA_FILES)
        self.hits: Counter[str] = Counter()
        self.offline = False

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handle))

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.offline:
            raise httpx.ConnectError("network is unreachable", request=request)
        url = request.url
        self.hits[url.host] += 1
        if url.host == "raw.githubusercontent.com":
            path = url.path.removeprefix(f"/{REPO}/{COMMIT}/")
            if path in self.raw:
                return httpx.Response(200, content=self.raw[path])
        if url.host == "media.githubusercontent.com":
            path = url.path.removeprefix(f"/media/{REPO}/{COMMIT}/")
            if path in self.media:
                return httpx.Response(200, content=self.media[path])
        return httpx.Response(404, text=f"not found: {url}")


def _entry(path: str, content: bytes, *, lfs: bool) -> FileEntry:
    found = re.match(r"^data/matches/(\d+)/", path)
    if path == "data/matches.json":
        role = "matches"
    elif path.startswith("data/aggregates/"):
        role = "aggregate"
    elif path.startswith("data/bodypose/"):
        role = "bodypose_sample"
    else:
        role = next(r for suffix, r in ROLE_BY_SUFFIX.items() if path.endswith(suffix))
    match_id = int(found.group(1)) if found else None
    return FileEntry(path, len(content), sha256(content), lfs, role, match_id)


def frozen_source() -> Source:
    """The manifest entry for the fake bundle, as it sits in sources.json."""
    files = [_entry(path, content, lfs=False) for path, content in RAW_FILES.items()]
    files += [_entry(path, blob, lfs=True) for path, blob in MEDIA_FILES.items()]
    return Source(
        source_id=SOURCE_ID,
        sport="football",
        kind="github",
        repo=REPO,
        ref="master",
        commit=COMMIT,
        license="MIT",
        raw_url=f"https://raw.githubusercontent.com/{REPO}/{{commit}}/",
        lfs_url=f"https://media.githubusercontent.com/media/{REPO}/{{commit}}/",
        files=sorted(files, key=lambda f: f.path),
    )


@pytest.fixture
def github() -> FakeGitHub:
    return FakeGitHub()


@pytest.fixture
def manifest_path(tmp_path: Path) -> Path:
    path = tmp_path / "manifest" / "sources.json"
    path.parent.mkdir(parents=True)
    manifest = {"schema_version": 1, "sources": [asdict(frozen_source())]}
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    return path


@pytest.fixture
def raw_root(tmp_path: Path) -> Path:
    return tmp_path / "data" / "raw" / SOURCE_ID


@pytest.fixture
def cli_client(github: FakeGitHub, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr("ftbase.cli.make_client", github.client)
    yield


def run_cli(manifest_path: Path, tmp_path: Path, *extra: str) -> int:
    return main(
        ["ingest", "--manifest", str(manifest_path), "--data-dir", str(tmp_path / "data"), *extra]
    )


def lay_out_clone(root: Path) -> None:
    """Write the fake bundle where a local clone of the provider's repo would have it."""
    for path, content in {**RAW_FILES, **MEDIA_FILES}.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_bytes(content)


def tree_state(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): sha256_file(p) for p in sorted(root.rglob("*")) if p.is_file()
    }


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
    n = BUNDLE_SIZE
    assert capsys.readouterr().out.splitlines()[-1] == f"downloaded={n} verified={n} failed=0"
    receipt = json.loads((raw_root / RECEIPT_NAME).read_text())
    assert receipt["failed"] == []
    assert (receipt["commit"], receipt["verified"], receipt["downloaded"]) == (COMMIT, n, n)
    assert receipt["match_id"] is None
    assert (raw_root / TRACKING).read_bytes() == TRACKING_BLOB
    assert (raw_root / MATCH_JSON).read_bytes() == RAW_FILES[MATCH_JSON]
    assert not list(raw_root.rglob("*.part"))

    github.hits.clear()
    assert run_cli(manifest_path, tmp_path) == 0
    assert capsys.readouterr().out.splitlines()[-1] == f"downloaded=0 verified={n} failed=0"
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
    assert out.splitlines()[-1] == f"downloaded=1 verified={BUNDLE_SIZE} failed=0"
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
    assert out.splitlines()[-1] == f"downloaded=0 verified={BUNDLE_SIZE - 1} failed=1"
    receipt = json.loads((raw_root / RECEIPT_NAME).read_text())
    assert receipt["failed"] == [TRACKING]
    assert not target.exists(), "a corrupt file must not survive as if it were good"
    assert not list(raw_root.rglob("*.part"))


def test_server_serving_wrong_bytes_is_rejected(github: FakeGitHub, raw_root: Path) -> None:
    """The hash check has teeth: bytes that do not match the manifest never land."""
    github.media[TRACKING] = TRACKING_BLOB[:-1] + bytes([TRACKING_BLOB[-1] ^ 0x01])
    source = frozen_source()
    logs: list[str] = []

    with github.client() as client:
        receipt = ingest_source(client, source, raw_root, log=logs.append)

    assert receipt.failed == [TRACKING]
    assert (receipt.downloaded, receipt.verified) == (BUNDLE_SIZE - 1, BUNDLE_SIZE - 1)
    assert any(line.startswith(f"failed: {TRACKING}") for line in logs)
    assert not (raw_root / TRACKING).exists()
    assert not list(raw_root.rglob("*.part"))
    others = [f for f in source.files if f.path != TRACKING]
    assert all(sha256_file(raw_root / f.path) == f.sha256 for f in others)


def test_ingest_refuses_a_source_without_files(tmp_path: Path, github: FakeGitHub) -> None:
    empty = frozen_source()
    empty.files = []
    with github.client() as client, pytest.raises(RuntimeError, match="no pinned commit or no"):
        ingest_source(client, empty, tmp_path, log=lambda _: None)


# --- --match: the fast path -----------------------------------------------------------


def test_select_match_keeps_the_four_files_and_the_index() -> None:
    chosen = select_match(frozen_source(), MATCH)
    assert len(chosen.files) == MATCH_SIZE
    assert {f.role for f in chosen.files} == {"match", "tracking", "events", "phases", "matches"}
    assert all(f.match_id in (MATCH, None) for f in chosen.files)


def test_select_match_rejects_an_unknown_id() -> None:
    with pytest.raises(KeyError, match="unknown match 42"):
        select_match(frozen_source(), 42)


def test_match_ingest_downloads_only_that_match_and_says_partial(
    manifest_path: Path,
    raw_root: Path,
    tmp_path: Path,
    cli_client: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert run_cli(manifest_path, tmp_path, "--match", str(MATCH)) == 0

    n = MATCH_SIZE
    expected = f"downloaded={n} verified={n} failed=0 PARTIAL {n}/{BUNDLE_SIZE}"
    assert capsys.readouterr().out.splitlines()[-1] == expected
    landed = {p.relative_to(raw_root).as_posix() for p in raw_root.rglob("*") if p.is_file()}
    assert landed == {f.path for f in select_match(frozen_source(), MATCH).files} | {RECEIPT_NAME}
    assert not (raw_root / OTHER_TRACKING).exists()
    receipt = json.loads((raw_root / RECEIPT_NAME).read_text())
    assert (receipt["match_id"], receipt["manifest_files"]) == (MATCH, BUNDLE_SIZE)


def test_match_ingest_with_unknown_id_exits_with_a_message(
    manifest_path: Path, tmp_path: Path, cli_client: None
) -> None:
    with pytest.raises(SystemExit, match="unknown match 42"):
        run_cli(manifest_path, tmp_path, "--match", "42")


# --- --from-existing: verify in place -----------------------------------------------------


def test_from_existing_verifies_in_place_and_never_writes_there(
    manifest_path: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    clone = tmp_path / "clone"
    lay_out_clone(clone)
    before = tree_state(clone)

    assert run_cli(manifest_path, tmp_path, "--from-existing", str(clone)) == 0

    n = BUNDLE_SIZE
    assert capsys.readouterr().out.splitlines()[-1] == f"downloaded=0 verified={n} failed=0"
    assert tree_state(clone) == before, "the clone must be left exactly as it was found"
    receipt = json.loads((tmp_path / "data" / "raw" / SOURCE_ID / RECEIPT_NAME).read_text())
    assert receipt["source_path"] == str(clone)


def test_from_existing_fails_on_a_changed_byte_and_on_a_missing_file(
    manifest_path: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    clone = tmp_path / "clone"
    lay_out_clone(clone)
    (clone / MATCH_JSON).write_bytes(RAW_FILES[MATCH_JSON].replace(b"Sydney", b"Sidney"))
    (clone / OTHER_TRACKING).unlink()

    assert run_cli(manifest_path, tmp_path, "--from-existing", str(clone)) == 1

    out = capsys.readouterr().out
    assert f"failed: {MATCH_JSON}" in out
    assert f"failed: {OTHER_TRACKING}" in out
    assert out.splitlines()[-1] == f"downloaded=0 verified={BUNDLE_SIZE - 2} failed=2"


def test_from_existing_names_crlf_as_the_cause(tmp_path: Path) -> None:
    clone = tmp_path / "clone"
    lay_out_clone(clone)
    (clone / MATCH_JSON).write_bytes(RAW_FILES[MATCH_JSON].replace(b"\n", b"\r\n"))
    logs: list[str] = []

    receipt = ingest_local(frozen_source(), clone, tmp_path / "receipt", log=logs.append)

    assert receipt.failed == [MATCH_JSON]
    assert "CRLF" in logs[0]


def test_from_existing_with_match_checks_only_that_match(
    manifest_path: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    clone = tmp_path / "clone"
    lay_out_clone(clone)
    (clone / OTHER_TRACKING).unlink()  # outside the chosen match: must not count
    args = ("--from-existing", str(clone), "--match", str(MATCH))

    assert run_cli(manifest_path, tmp_path, *args) == 0

    n = MATCH_SIZE
    expected = f"downloaded=0 verified={n} failed=0 PARTIAL {n}/{BUNDLE_SIZE}"
    assert capsys.readouterr().out.splitlines()[-1] == expected


def test_shipped_manifest_pins_the_85_football_files() -> None:
    source = load_manifest(default_manifest()).source(SOURCE_ID)
    assert source.commit == "4340d274572876239c154c90bc507a9b3250a656"
    assert len(source.files) == 85
    assert sum(f.lfs for f in source.files) == 20
    assert len(select_match(source, 1874553).files) == MATCH_SIZE
