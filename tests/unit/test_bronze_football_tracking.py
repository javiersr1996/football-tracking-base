"""bronze football tracking on a synthetic two-match bundle. No real data, no network.

The bundle is built to a known truth: an out-of-period frame (`period`/`timestamp`/
`possession`/`image_corners_projection` all null, `player_data` empty, `ball_data`
all-null), a full in-period frame (players + ball + corners), an empty in-period frame
(0 players, ball absent, but `period`/`timestamp` set), a jump in `frame` that must
stay a jump, and player `is_detected` delivered as a native JSON boolean (not 0/1, the
opposite of basketball's `isDetected`). Every row and type of both Parquet tables is
then asserted, and each way the input can break the declared schema or the frozen key
sets is fed to the builder to prove it fails with the column, key or line in the
message and leaves no Parquet behind.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from manifest_helper import save_manifest

from ftbase.bronze.common import raw_root_for
from ftbase.bronze.football.tracking import (
    CORNER_ORDER,
    TRACKING_FRAMES,
    TRACKING_OBJECTS,
    build_tracking_match,
    frame_batches,
)
from ftbase.cli import main
from ftbase.ingest.fetch import RECEIPT_NAME
from ftbase.ingest.manifest import FileEntry, Manifest, Source

SOURCE_ID = "opendata-football"
COMMIT = "4340d274572876239c154c90bc507a9b3250a656"
MATCH_A, MATCH_B = 8000001, 8000002

NULL_BALL: dict[str, Any] = {"x": None, "y": None, "z": None, "is_detected": None}
NULL_POSSESSION: dict[str, Any] = {"group": None, "player_id": None}
NULL_CORNERS: dict[str, Any] = dict.fromkeys(CORNER_ORDER)
FULL_CORNERS: dict[str, Any] = {name: 10.0 + i for i, name in enumerate(CORNER_ORDER)}
BALL_PRESENT: dict[str, Any] = {"x": 0.5, "y": -0.2, "z": 0.19, "is_detected": True}
POSSESSION_HOME: dict[str, Any] = {"group": "home team", "player_id": 301}
PLAYERS_3: list[dict[str, Any]] = [
    {"player_id": 301, "x": -10.0, "y": 1.0, "is_detected": True},
    {"player_id": 302, "x": 5.0, "y": -2.0, "is_detected": False},
    {"player_id": 303, "x": 20.0, "y": 3.0, "is_detected": True},
]


def tracking_path(match_id: int) -> str:
    return f"data/matches/{match_id}/{match_id}_tracking_extrapolated.jsonl"


def frame(
    idx: int,
    period: int | None,
    timestamp: str | None,
    players: list[dict[str, Any]],
    ball: dict[str, Any],
    possession: dict[str, Any],
    corners: dict[str, Any],
) -> dict[str, Any]:
    return {
        "frame": idx,
        "timestamp": timestamp,
        "period": period,
        "ball_data": dict(ball),
        "possession": dict(possession),
        "image_corners_projection": dict(corners),
        "player_data": [dict(p) for p in players],
    }


def out_of_period(idx: int) -> dict[str, Any]:
    return frame(idx, None, None, [], NULL_BALL, NULL_POSSESSION, NULL_CORNERS)


def full_frame(idx: int, timestamp: str) -> dict[str, Any]:
    return frame(idx, 1, timestamp, PLAYERS_3, BALL_PRESENT, POSSESSION_HOME, FULL_CORNERS)


def empty_in_period(idx: int, timestamp: str) -> dict[str, Any]:
    return frame(idx, 1, timestamp, [], NULL_BALL, NULL_POSSESSION, NULL_CORNERS)


# Line 3 is frame 600: a jump of 598, like a break between periods. Bronze keeps it a jump.
FRAMES_A = [
    out_of_period(0),
    full_frame(1, "00:00:00.00"),
    empty_in_period(2, "00:00:00.10"),
    full_frame(600, "00:00:59.00"),
    empty_in_period(601, "00:00:59.10"),
]
FRAMES_B = [full_frame(0, "00:00:00.00"), out_of_period(1)]


def jsonl_bytes(frames: list[dict[str, Any]]) -> bytes:
    return "".join(json.dumps(f) + "\n" for f in frames).encode("utf-8")


def write_jsonl(path: Path, frames: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(jsonl_bytes(frames))


def entry_for(match_id: int, blob: bytes) -> FileEntry:
    return FileEntry(
        path=tracking_path(match_id),
        bytes=len(blob),
        sha256=hashlib.sha256(blob).hexdigest(),
        lfs=True,
        role="tracking",
        match_id=match_id,
    )


def frozen_source(files: list[FileEntry]) -> Source:
    repo = "SkillCorner/opendata"
    return Source(
        source_id=SOURCE_ID,
        sport="football",
        kind="github",
        repo=repo,
        ref="master",
        commit=COMMIT,
        license="MIT",
        raw_url=f"https://raw.githubusercontent.com/{repo}/{{commit}}/",
        lfs_url=f"https://media.githubusercontent.com/media/{repo}/{{commit}}/",
        files=files,
    )


def write_receipt(data_dir: Path, source_path: Path | None, failed: list[str]) -> None:
    receipt_dir = data_dir / "raw" / SOURCE_ID
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt = {
        "source_id": SOURCE_ID,
        "commit": COMMIT,
        "verified": 2,
        "downloaded": 0,
        "failed": failed,
        "finished_at": "2026-09-22T10:00:00+00:00",
        "source_path": str(source_path) if source_path else None,
    }
    (receipt_dir / RECEIPT_NAME).write_text(json.dumps(receipt), encoding="utf-8")


@pytest.fixture
def bundle(tmp_path: Path) -> tuple[Path, Path]:
    """(manifest path, data dir): two matches in an outside clone, verified by receipt."""
    clone = tmp_path / "clone"
    data_dir = tmp_path / "data"
    entries = []
    for match_id, frames in ((MATCH_A, FRAMES_A), (MATCH_B, FRAMES_B)):
        blob = jsonl_bytes(frames)
        path = clone / tracking_path(match_id)
        path.parent.mkdir(parents=True)
        path.write_bytes(blob)
        entries.append(entry_for(match_id, blob))
    # The manifest also lists a non-tracking file: the builder must not touch it.
    entries.append(FileEntry("data/matches.json", 2, "00" * 32, False, "matches", None))
    manifest_path = tmp_path / "sources.json"
    save_manifest(Manifest(schema_version=1, sources=[frozen_source(entries)]), manifest_path)
    write_receipt(data_dir, clone, failed=[])
    return manifest_path, data_dir


def rows(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


def run_tracking(manifest_path: Path, data_dir: Path) -> int:
    return main(
        [
            "bronze",
            "--tables",
            "tracking",
            "--manifest",
            str(manifest_path),
            "--data-dir",
            str(data_dir),
        ]
    )


# --- the CLI on the synthetic bundle -------------------------------------------------


def test_cli_builds_both_tables_per_match(
    bundle: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path, data_dir = bundle
    rc = run_tracking(manifest_path, data_dir)
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("bronze tracking: ")
    assert out[1].split() == ["match_id", "frames", "objects", "seconds", "MB"]
    assert out[2].split()[:3] == [str(MATCH_A), "5", "6"], "3 players in frame 1 and frame 600"
    assert out[3].split()[:3] == [str(MATCH_B), "2", "3"]
    assert out[4].startswith("total seconds=")

    for table in ("tracking_frames", "tracking_objects"):
        names = sorted(p.name for p in (data_dir / "bronze" / "football" / table).iterdir())
        assert names == [f"{MATCH_A}.parquet", f"{MATCH_B}.parquet"], "one file per match"


def test_schema_metadata_and_lineage(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    run_tracking(manifest_path, data_dir)
    frames_path = data_dir / "bronze" / "football" / "tracking_frames" / f"{MATCH_A}.parquet"
    objects_path = data_dir / "bronze" / "football" / "tracking_objects" / f"{MATCH_A}.parquet"

    for path, declared in ((frames_path, TRACKING_FRAMES), (objects_path, TRACKING_OBJECTS)):
        written = pq.read_schema(path)
        assert written.equals(declared, check_metadata=False), f"{path.name}: {written}"
        meta = {k.decode(): v.decode() for k, v in written.metadata.items() if k != b"ARROW:schema"}
        assert meta == {
            "ftbase.sport": "football",
            "ftbase.source_file": tracking_path(MATCH_A),
            "ftbase.source_sha256": hashlib.sha256(jsonl_bytes(FRAMES_A)).hexdigest(),
            "ftbase.source_commit": COMMIT,
        }

    for row in rows(frames_path) + rows(objects_path):
        assert (row["sport"], row["match_id"], row["source_file"]) == (
            "football",
            MATCH_A,
            tracking_path(MATCH_A),
        )


def test_frames_keep_the_gap_and_out_of_period_nulls(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    run_tracking(manifest_path, data_dir)
    frames = rows(data_dir / "bronze" / "football" / "tracking_frames" / f"{MATCH_A}.parquet")

    assert [f["frame"] for f in frames] == [0, 1, 2, 600, 601], "no frame invented in the gap"
    assert [f["source_row"] for f in frames] == [0, 1, 2, 3, 4], "line N is not frame N"
    assert [f["period"] for f in frames] == [None, 1, 1, 1, 1]
    assert [f["timestamp"] for f in frames] == [
        None,
        "00:00:00.00",
        "00:00:00.10",
        "00:00:59.00",
        "00:00:59.10",
    ]
    assert [f["n_players"] for f in frames] == [0, 3, 0, 3, 0]
    assert [f["ball_is_detected"] for f in frames] == [None, True, None, True, None]
    assert [f["x_bottom_left"] for f in frames] == [None, 10.0, None, 10.0, None]
    assert [f["possession_group"] for f in frames] == [None, "home team", None, "home team", None]

    out_of_period_row = frames[0]
    assert (
        out_of_period_row["ball_x"],
        out_of_period_row["ball_y"],
        out_of_period_row["ball_z"],
    ) == (
        None,
        None,
        None,
    )
    full_row = frames[1]
    assert (full_row["ball_x"], full_row["ball_y"], full_row["ball_z"]) == (0.5, -0.2, 0.19)


def test_objects_are_typed_as_delivered(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    run_tracking(manifest_path, data_dir)
    objects = rows(data_dir / "bronze" / "football" / "tracking_objects" / f"{MATCH_A}.parquet")

    assert len(objects) == 6, "3 per full frame, nothing for the 3 frames with 0 players"
    assert sorted({o["frame"] for o in objects}) == [1, 600]
    assert {o["source_row"] for o in objects if o["frame"] == 600} == {3}

    frame1 = [o for o in objects if o["frame"] == 1]
    assert [o["player_id"] for o in frame1] == [301, 302, 303]
    assert [o["is_detected"] for o in frame1] == [True, False, True]
    assert all(isinstance(o["is_detected"], bool) for o in frame1), "native bool, no 0/1 cast"
    assert (frame1[0]["x"], frame1[0]["y"]) == (-10.0, 1.0)


# --- every way the input can break the declared schema or the frozen key sets --------


def broken(mutate: Any) -> tuple[dict[str, Any], str]:
    """One full frame with `mutate(frame)` applied, and the source_file it belongs to."""
    f = full_frame(1, "00:00:00.00")
    mutate(f)
    return f, tracking_path(MATCH_A)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda f: f.__setitem__("extra", 1), "frame keys"),
        (lambda f: f.pop("possession"), "frame keys"),
        (lambda f: f["ball_data"].pop("z"), "ball_data keys"),
        (lambda f: f["possession"].__setitem__("extra", 1), "possession keys"),
        (
            lambda f: f["image_corners_projection"].pop("x_top_left"),
            "image_corners_projection keys",
        ),
        (lambda f: f["player_data"][0].__setitem__("extra", 1), "player keys"),
        (lambda f: f["player_data"][1].pop("is_detected"), "player keys"),
        (
            lambda f: f["player_data"][0].__setitem__("is_detected", None),
            r"column is_detected: 1 null\(s\), declared NOT NULL",
        ),
        (lambda f: f.__setitem__("frame", "not-an-int"), "column frame"),
    ],
)
def test_schema_violations_fail_with_the_name(tmp_path: Path, mutate: Any, message: str) -> None:
    f, source_file = broken(mutate)
    path = tmp_path / "broken.jsonl"
    write_jsonl(path, [f])
    with pytest.raises(ValueError, match=message):
        list(frame_batches(path, MATCH_A, source_file))


def test_a_failed_match_leaves_no_parquet_behind(tmp_path: Path) -> None:
    clone = tmp_path / "clone"
    data_dir = tmp_path / "data"
    bad, _ = broken(lambda f: f["player_data"][0].__setitem__("extra", 1))
    frames = [out_of_period(0), bad]
    write_jsonl(clone / tracking_path(MATCH_A), frames)
    entry = entry_for(MATCH_A, jsonl_bytes(frames))

    with pytest.raises(ValueError, match="line 1: player keys"):
        build_tracking_match(entry, clone, data_dir, COMMIT)

    for table in ("tracking_frames", "tracking_objects"):
        assert list((data_dir / "bronze" / "football" / table).iterdir()) == []


def test_raw_root_from_receipt(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    with pytest.raises(FileNotFoundError, match="ftbase ingest"):
        raw_root_for(data_dir, SOURCE_ID)
    write_receipt(data_dir, tmp_path / "clone", failed=[])
    assert raw_root_for(data_dir, SOURCE_ID) == tmp_path / "clone"
