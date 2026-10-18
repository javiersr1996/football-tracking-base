"""Contract of the bronze football tracking against the built Parquet and the raw JSONL.

Marked `realdata` (deselected by default, never skipped): run them with
`FTBASE_DATA_ROOT=<dir> uv run pytest -m realdata` after `ftbase ingest` and `ftbase bronze`
on the real bundle. Everything here reads
the artefact on disk with DuckDB or pyarrow, never what the builder computed, and
prints its evidence per match, never an aggregate. The independent counter of the last
test walks the JSONL with ``open`` and ``json.loads`` alone, no pyarrow and none of the
tracking builder's own parsing code, so it shares no line with the writer it checks.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from ftbase.bronze.common import raw_root_for
from ftbase.bronze.football.tracking import TABLES
from ftbase.cli import default_manifest
from ftbase.ingest.manifest import load_manifest
from ftbase.paths import data_root

DATA = data_root()
BRONZE = DATA / "bronze" / "football"
SOURCE_ID = "opendata-football"
COUNTED_MATCH = 1925299
EXPECTED_FRAMES_TOTAL = 1_244_814
EXPECTED_OBJECTS_TOTAL = 19_334_986
EXPECTED_COUNTED_OBJECTS = 1_054_724

pytestmark = pytest.mark.realdata


@pytest.fixture(scope="module")
def match_ids() -> list[int]:
    source = load_manifest(default_manifest()).source(SOURCE_ID)
    ids = sorted(f.match_id for f in source.files if f.role == "tracking" and f.match_id)
    assert len(ids) == 20, f"expected 20 tracking files in the manifest, found {len(ids)}"
    return ids


def parquet(table: str, match_id: int) -> str:
    path = BRONZE / table / f"{match_id}.parquet"
    assert path.is_file(), f"{path} missing"
    return path.as_posix()


def test_every_file_has_the_declared_schema(match_ids: list[int]) -> None:
    for table, declared in TABLES.items():
        for match_id in match_ids:
            written = pq.read_schema(parquet(table, match_id))
            print(f"{table} {match_id} columns={len(written)}")
            assert written.equals(declared, check_metadata=False), f"{table} {match_id}: {written}"


def test_frame_rows_equal_frame_max_plus_1(match_ids: list[int]) -> None:
    """``frame`` is contiguous from 0 (task 4): row count must equal ``max(frame) + 1``."""
    con = duckdb.connect()
    print(f"\n{'match_id':>8} {'frames':>8} {'frame_max+1':>12}")
    mismatches = []
    total = 0
    for match_id in match_ids:
        n, frame_max = con.execute(
            "SELECT count(*), max(frame) FROM read_parquet(?)",
            [parquet("tracking_frames", match_id)],
        ).fetchone()
        total += n
        print(f"{match_id:>8} {n:>8} {frame_max + 1:>12}")
        if n != frame_max + 1:
            mismatches.append(match_id)
    print(f"total frames={total}")
    assert mismatches == []
    assert total == EXPECTED_FRAMES_TOTAL


def test_object_rows_total(match_ids: list[int]) -> None:
    con = duckdb.connect()
    print(f"\n{'match_id':>8} {'objects':>9}")
    total = 0
    for match_id in match_ids:
        (n,) = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [parquet("tracking_objects", match_id)]
        ).fetchone()
        total += n
        print(f"{match_id:>8} {n:>9}")
    print(f"total objects={total}")
    assert total == EXPECTED_OBJECTS_TOTAL


def test_in_period_frames_are_full_or_empty(match_ids: list[int]) -> None:
    """Inside a period: 22 players or 0, never 1-21 and never more than 22 (task 4)."""
    con = duckdb.connect()
    print(f"\n{'match_id':>8} {'in_period':>9} {'full22':>8} {'empty0':>8} {'partial':>8}")
    partials = []
    for match_id in match_ids:
        n, full, empty = con.execute(
            "SELECT count(*) FILTER (WHERE period IS NOT NULL), "
            "count(*) FILTER (WHERE period IS NOT NULL AND n_players = 22), "
            "count(*) FILTER (WHERE period IS NOT NULL AND n_players = 0) "
            "FROM read_parquet(?)",
            [parquet("tracking_frames", match_id)],
        ).fetchone()
        partial = n - full - empty
        print(f"{match_id:>8} {n:>9} {full:>8} {empty:>8} {partial:>8}")
        if partial:
            partials.append(match_id)
    assert partials == []


def test_timestamp_null_iff_period_null(match_ids: list[int]) -> None:
    con = duckdb.connect()
    print(f"\n{'match_id':>8} {'mismatches':>10}")
    bad = []
    for match_id in match_ids:
        (n,) = con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE (timestamp IS NULL) != (period IS NULL)",
            [parquet("tracking_frames", match_id)],
        ).fetchone()
        print(f"{match_id:>8} {n:>10}")
        if n:
            bad.append(match_id)
    assert bad == []


def test_ball_z_nonnull_iff_ball_present(match_ids: list[int]) -> None:
    """``ball_data`` is present exactly when the 22 players are (task 4): ``ball_z`` non-null
    iff ``n_players = 22``, checked inside every period."""
    con = duckdb.connect()
    print(f"\n{'match_id':>8} {'mismatches':>10}")
    bad = []
    for match_id in match_ids:
        (n,) = con.execute(
            "SELECT count(*) FROM read_parquet(?) "
            "WHERE period IS NOT NULL AND (ball_z IS NOT NULL) != (n_players = 22)",
            [parquet("tracking_frames", match_id)],
        ).fetchone()
        print(f"{match_id:>8} {n:>10}")
        if n:
            bad.append(match_id)
    assert bad == []


def test_independent_count_of_lines_and_objects_for_1925299(match_ids: list[int]) -> None:
    """``open`` + ``json.loads`` walk of the raw JSONL of 1925299: lines must equal the rows
    of ``tracking_frames``, and the sum of ``len(player_data)`` the rows of
    ``tracking_objects``."""
    assert COUNTED_MATCH in match_ids
    raw_root = raw_root_for(DATA, SOURCE_ID)
    path = (
        raw_root
        / "data"
        / "matches"
        / str(COUNTED_MATCH)
        / f"{COUNTED_MATCH}_tracking_extrapolated.jsonl"
    )

    lines = 0
    objects = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            d = json.loads(line)
            lines += 1
            objects += len(d["player_data"])

    con = duckdb.connect()
    (frame_rows,) = con.execute(
        "SELECT count(*) FROM read_parquet(?)", [parquet("tracking_frames", COUNTED_MATCH)]
    ).fetchone()
    (object_rows,) = con.execute(
        "SELECT count(*) FROM read_parquet(?)", [parquet("tracking_objects", COUNTED_MATCH)]
    ).fetchone()
    print(f"\n{COUNTED_MATCH}: file lines={lines} tracking_frames rows={frame_rows}")
    print(f"{COUNTED_MATCH}: file objects={objects} tracking_objects rows={object_rows}")
    assert frame_rows == lines
    assert object_rows == objects
    assert object_rows == EXPECTED_COUNTED_OBJECTS
