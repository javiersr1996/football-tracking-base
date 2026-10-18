# Rewritten from tracklab@686508a: pipeline/tests/contracts/test_football_events_bronze.py
# Changes: imports, data root and the `realdata` marker as in the other contract tests;
# a header-hash test added (`test_raw_headers_match_the_frozen_header_sha256`); comments
# no longer cite internal task numbers.
"""Contract of the bronze football events (`dynamic_events`, `phases_of_play`)
against the built Parquet and the raw CSVs.

Marked `realdata` (deselected by default, never skipped): run them with
`FTBASE_DATA_ROOT=<dir> uv run pytest -m realdata` after `ftbase ingest` and `ftbase bronze`
on the real bundle. Everything here reads
the artefact on disk with DuckDB or pyarrow, never what the builder computed in
memory, and prints its evidence per match, never an aggregate. The row-count test
walks each raw CSV with plain `open()` and counts lines, no `csv` module, no
pyarrow, no DuckDB, no import of `ftbase.bronze.football.events`: it shares no
line with the writer it checks. `dynamic_events` and `phases_of_play` rows equal the
file's line count minus 1 (the header) in all 20 matches; this test proves it against the
actual artefact.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from ftbase.bronze.common import LINEAGE_FIELDS, raw_root_for
from ftbase.bronze.football.events import load_frozen_tables, table_schema
from ftbase.cli import default_manifest
from ftbase.ingest.manifest import load_manifest
from ftbase.paths import data_root

DATA = data_root()
BRONZE = DATA / "bronze" / "football"
SOURCE_ID = "opendata-football"

# Counted directly on the 20 raw CSVs before the bronze writer existed; this test
# re-derives the per-match rows independently and checks the totals against these numbers.
EXPECTED_TOTAL = {"dynamic_events": 94_361, "phases_of_play": 8_834}

# 322 and 44 columns, frozen once. Both CSVs carry a
# native `match_id` column, the same name `bronze.common.LINEAGE_FIELDS` already
# reserves for the match id every table starts with; a pyarrow schema cannot
# usefully carry two fields of one name, so `events.py` folds the native one into
# the lineage one instead of writing it twice (see its module docstring). Written
# columns are therefore lineage + frozen columns minus that one fold, spelled out
# here so the subtraction is explicit instead of hidden behind a single total.
FROZEN_COLUMNS = {"dynamic_events": 322, "phases_of_play": 44}
FOLDED_NATIVE_COLUMNS = 1  # match_id
EXPECTED_WRITTEN_COLUMNS = {
    table: len(LINEAGE_FIELDS) + n - FOLDED_NATIVE_COLUMNS for table, n in FROZEN_COLUMNS.items()
}
ORPHAN_MATCH = 1953632
ORPHAN_PLAYER_ID = 973077
ORPHAN_ROWS = 35
EXPECTED_EVENT_TYPE_TOTALS = {
    "passing_option": 48_308,
    "player_possession": 18_710,
    "on_ball_engagement": 17_445,
    "off_ball_run": 9_898,
}

pytestmark = pytest.mark.realdata


@pytest.fixture(scope="module")
def match_ids() -> list[int]:
    source = load_manifest(default_manifest()).source(SOURCE_ID)
    ids = sorted(f.match_id for f in source.files if f.role == "events" and f.match_id)
    assert len(ids) == 20, f"expected 20 dynamic_events files in the manifest, found {len(ids)}"
    return ids


def parquet(table: str, match_id: int) -> str:
    path = BRONZE / table / f"{match_id}.parquet"
    assert path.is_file(), f"{path} missing"
    return path.as_posix()


def raw_csv(table: str, match_id: int) -> Path:
    raw_root = raw_root_for(DATA, SOURCE_ID)
    return raw_root / "data" / "matches" / str(match_id) / f"{match_id}_{table}.csv"


def test_written_column_counts_are_325_and_47() -> None:
    """Literal figures, not just the derivation: 4 lineage + 322 frozen - 1 folded
    native match_id = 325 for `dynamic_events`; 4 + 44 - 1 = 47 for
    `phases_of_play`."""
    assert EXPECTED_WRITTEN_COLUMNS == {"dynamic_events": 325, "phases_of_play": 47}


def test_every_file_has_the_declared_schema(match_ids: list[int]) -> None:
    frozen = load_frozen_tables()
    for table in ("dynamic_events", "phases_of_play"):
        assert len(frozen[table].columns) == FROZEN_COLUMNS[table], (
            f"{table}: the frozen *.schema.json itself must still declare "
            f"{FROZEN_COLUMNS[table]} columns (the fold only affects what bronze "
            f"writes, never the frozen file)"
        )
        declared = table_schema(frozen[table])
        expected = len(LINEAGE_FIELDS) + FROZEN_COLUMNS[table] - FOLDED_NATIVE_COLUMNS
        assert expected == EXPECTED_WRITTEN_COLUMNS[table]
        assert len(declared) == expected, (
            f"{table}: declared {len(declared)} columns, expected "
            f"{len(LINEAGE_FIELDS)} lineage + {FROZEN_COLUMNS[table]} frozen - "
            f"{FOLDED_NATIVE_COLUMNS} folded native match_id = {expected}"
        )
        for match_id in match_ids:
            written = pq.read_schema(parquet(table, match_id))
            print(f"{table} {match_id} columns={len(written)}")
            assert len(written) == expected, f"{table} {match_id}: {len(written)} != {expected}"
            assert written.equals(declared, check_metadata=False), f"{table} {match_id}: {written}"


def test_row_counts_equal_csv_lines_minus_header(match_ids: list[int]) -> None:
    """Independent counter: plain `open()` line counting, no csv/duckdb/pyarrow."""
    con = duckdb.connect()
    totals = dict.fromkeys(EXPECTED_TOTAL, 0)
    print(f"\n{'match_id':>8} {'table':>15} {'csv_lines-1':>11} {'parquet_rows':>12}")
    mismatches = []
    for match_id in match_ids:
        for table in ("dynamic_events", "phases_of_play"):
            with raw_csv(table, match_id).open("r", encoding="utf-8") as fh:
                csv_rows = sum(1 for _ in fh) - 1
            (n,) = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [parquet(table, match_id)]
            ).fetchone()
            totals[table] += n
            print(f"{match_id:>8} {table:>15} {csv_rows:>11} {n:>12}")
            if n != csv_rows:
                mismatches.append((table, match_id))
    print(f"totals: {totals}")
    assert mismatches == []
    assert totals == EXPECTED_TOTAL


def test_source_row_is_0_based_and_contiguous(match_ids: list[int]) -> None:
    """`source_row` is the CSV's 0-based data row, the lineage
    contract: `[0, n)` with no gap, checked per match and table."""
    con = duckdb.connect()
    bad = []
    for match_id in match_ids:
        for table in ("dynamic_events", "phases_of_play"):
            rows = con.execute(
                "SELECT source_row FROM read_parquet(?) ORDER BY source_row",
                [parquet(table, match_id)],
            ).fetchall()
            observed = [r[0] for r in rows]
            expected = list(range(len(rows)))
            if observed != expected:
                bad.append((table, match_id))
    assert bad == []


def test_event_type_counts(match_ids: list[int]) -> None:
    """Only the 4 known `event_type` values may appear, per match and in total."""
    con = duckdb.connect()
    totals = dict.fromkeys(EXPECTED_EVENT_TYPE_TOTALS, 0)
    header = " ".join(f"{t:>18}" for t in EXPECTED_EVENT_TYPE_TOTALS)
    print(f"\n{'match_id':>8} {header}")
    for match_id in match_ids:
        rows = con.execute(
            "SELECT event_type, count(*) FROM read_parquet(?) GROUP BY 1",
            [parquet("dynamic_events", match_id)],
        ).fetchall()
        counts = dict(rows)
        unknown = set(counts) - set(EXPECTED_EVENT_TYPE_TOTALS)
        assert not unknown, f"{match_id}: unknown event_type {unknown}"
        line = " ".join(f"{counts.get(t, 0):>18}" for t in EXPECTED_EVENT_TYPE_TOTALS)
        print(f"{match_id:>8} {line}")
        for t, n in counts.items():
            totals[t] += n
    print(f"totals: {totals}")
    assert totals == EXPECTED_EVENT_TYPE_TOTALS


def test_orphan_player_rows_are_present(match_ids: list[int]) -> None:
    """`events.player_id` is not an enforceable foreign
    key. Match 1953632's `player_id` 973077 (35 rows, absent from `players[]` and
    the tracking) must be loaded, never dropped."""
    assert ORPHAN_MATCH in match_ids
    con = duckdb.connect()
    (n,) = con.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE player_id = ?",
        [parquet("dynamic_events", ORPHAN_MATCH), ORPHAN_PLAYER_ID],
    ).fetchone()
    print(f"\n{ORPHAN_MATCH} player_id={ORPHAN_PLAYER_ID}: rows={n}")
    assert n == ORPHAN_ROWS


def test_raw_headers_match_the_frozen_header_sha256(match_ids: list[int]) -> None:
    """The first line of every raw CSV hashes to the `header_sha256` frozen in the schema
    (plain `open()` and `hashlib`, none of csvschema's code), per match and table."""
    frozen = load_frozen_tables()
    print(f"\n{'match_id':>8} {'table':>15} {'header matches':>14}")
    bad = []
    for match_id in match_ids:
        for table, schema in frozen.items():
            with raw_csv(table, match_id).open(encoding="utf-8", newline="") as fh:
                header = fh.readline().rstrip("\r\n")
            ok = hashlib.sha256(header.encode("utf-8")).hexdigest() == schema.header_sha256
            print(f"{match_id:>8} {table:>15} {ok!s:>14}")
            if not ok:
                bad.append((match_id, table))
    assert bad == []
