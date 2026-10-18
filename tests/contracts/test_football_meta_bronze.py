# Copied from tracklab@686508a: pipeline/tests/contracts/test_football_meta_bronze.py
# Changes: imports and data root (ftbase, `FTBASE_DATA_ROOT`); the `realdata` marker instead of a
# skip when bronze is missing; the docstring no longer cite internal task numbers.
"""Contract of the bronze football meta against the built Parquet and the raw match.json.

Marked `realdata` (deselected by default, never skipped): run them with
`FTBASE_DATA_ROOT=<dir> uv run pytest -m realdata` after `ftbase ingest` and `ftbase bronze`.

Two things this file is the only place that checks: ``match_periods`` really has 40 rows
(20 matches x 2 periods), and the leaf checker:
every scalar value ``match.json`` delivers, in every one of the 20 matches, is
represented in a column of ``match``, ``match_periods`` or ``match_players``. The
checker walks the raw JSON directly (no ftbase import in the walker itself) and
compares against ``COVERED_LEAF_PATTERNS``, a hand-maintained mirror of the flattening
``bronze/football/meta.py`` performs; a path missing from that set means a value bronze
silently drops, and the test prints it instead of passing.
"""

from __future__ import annotations

import json
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest

from ftbase.bronze.common import raw_root_for
from ftbase.bronze.football.meta import TABLES
from ftbase.cli import default_manifest
from ftbase.ingest.manifest import load_manifest
from ftbase.paths import data_root

DATA = data_root()
BRONZE = DATA / "bronze" / "football"
SOURCE_ID = "opendata-football"
EXPECTED_MATCH_PERIODS_ROWS = 40  # 20 matches x 2 periods

pytestmark = pytest.mark.realdata


@pytest.fixture(scope="module")
def match_ids() -> list[int]:
    source = load_manifest(default_manifest()).source(SOURCE_ID)
    ids = sorted(f.match_id for f in source.files if f.role == "match" and f.match_id)
    assert len(ids) == 20, f"expected 20 match.json files in the manifest, found {len(ids)}"
    return ids


def parquet(table: str, match_id: int | None = None) -> str:
    path = BRONZE / table / (f"{match_id}.parquet" if match_id is not None else "data.parquet")
    assert path.is_file(), f"{path} missing"
    return path.as_posix()


def test_every_file_has_the_declared_schema(match_ids: list[int]) -> None:
    for table in ("match", "match_periods", "match_players"):
        declared = TABLES[table]
        for match_id in match_ids:
            written = pq.read_schema(parquet(table, match_id))
            print(f"{table} {match_id} columns={len(written)}")
            assert written.equals(declared, check_metadata=False), f"{table} {match_id}: {written}"
    written = pq.read_schema(parquet("matches"))
    assert written.equals(TABLES["matches"], check_metadata=False), written


def test_match_periods_has_40_rows_2_per_match(match_ids: list[int]) -> None:
    con = duckdb.connect()
    print(f"\n{'match_id':>8} {'periods':>8}")
    total = 0
    for match_id in match_ids:
        (n,) = con.execute(
            "SELECT count(*) FROM read_parquet(?)", [parquet("match_periods", match_id)]
        ).fetchone()
        total += n
        print(f"{match_id:>8} {n:>8}")
        assert n == 2, f"{match_id}: expected 2 periods, found {n}"
    print(f"total match_periods rows={total}")
    assert total == EXPECTED_MATCH_PERIODS_ROWS


# --- the leaf checker: every scalar value of match.json lands in a column ----


def scalar_leaf_paths(obj: Any, prefix: str = "$") -> set[str]:
    """Every JSON path that holds a scalar (or null) value, arrays generalised to ``[]``.

    An empty list or an empty dict contributes no path: it carries no scalar value to
    lose. This is a plain, dependency-free walk, deliberately not shared with the
    builder it checks.
    """
    paths: set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            paths |= scalar_leaf_paths(v, f"{prefix}.{k}")
    elif isinstance(obj, list):
        for item in obj:
            paths |= scalar_leaf_paths(item, f"{prefix}[]")
    else:
        paths.add(prefix)
    return paths


COVERED_LEAF_PATTERNS = frozenset(
    {
        # match
        "$.id",
        "$.home_team_score",
        "$.away_team_score",
        "$.date_time",
        "$.stadium.id",
        "$.stadium.name",
        "$.stadium.city",
        "$.stadium.capacity",
        "$.home_team.id",
        "$.home_team.name",
        "$.home_team.short_name",
        "$.home_team.acronym",
        "$.away_team.id",
        "$.away_team.name",
        "$.away_team.short_name",
        "$.away_team.acronym",
        "$.home_team_kit.id",
        "$.home_team_kit.team_id",
        "$.home_team_kit.name",
        "$.home_team_kit.jersey_color",
        "$.home_team_kit.number_color",
        "$.home_team_kit.season.id",
        "$.home_team_kit.season.start_year",
        "$.home_team_kit.season.end_year",
        "$.home_team_kit.season.name",
        "$.away_team_kit.id",
        "$.away_team_kit.team_id",
        "$.away_team_kit.name",
        "$.away_team_kit.jersey_color",
        "$.away_team_kit.number_color",
        "$.away_team_kit.season.id",
        "$.away_team_kit.season.start_year",
        "$.away_team_kit.season.end_year",
        "$.away_team_kit.season.name",
        "$.home_team_coach",
        "$.away_team_coach",
        "$.home_team_playing_time.minutes_tip",
        "$.home_team_playing_time.minutes_otip",
        "$.away_team_playing_time.minutes_tip",
        "$.away_team_playing_time.minutes_otip",
        "$.competition_edition.id",
        "$.competition_edition.name",
        "$.competition_edition.competition.id",
        "$.competition_edition.competition.area",
        "$.competition_edition.competition.name",
        "$.competition_edition.competition.gender",
        "$.competition_edition.competition.age_group",
        "$.competition_edition.season.id",
        "$.competition_edition.season.start_year",
        "$.competition_edition.season.end_year",
        "$.competition_edition.season.name",
        "$.competition_round.id",
        "$.competition_round.name",
        "$.competition_round.round_number",
        "$.competition_round.potential_overtime",
        "$.status",
        "$.home_team_side[]",
        "$.ball.trackable_object",
        "$.pitch_length",
        "$.pitch_width",
        # match_periods
        "$.match_periods[].period",
        "$.match_periods[].name",
        "$.match_periods[].start_frame",
        "$.match_periods[].end_frame",
        "$.match_periods[].duration_frames",
        "$.match_periods[].duration_minutes",
        # match_players
        "$.players[].id",
        "$.players[].team_id",
        "$.players[].team_player_id",
        "$.players[].trackable_object",
        "$.players[].first_name",
        "$.players[].last_name",
        "$.players[].short_name",
        "$.players[].birthday",
        "$.players[].gender",
        "$.players[].number",
        "$.players[].start_time",
        "$.players[].end_time",
        "$.players[].yellow_card",
        "$.players[].red_card",
        "$.players[].injured",
        "$.players[].goal",
        "$.players[].own_goal",
        "$.players[].last_modified",
        "$.players[].player_role.id",
        "$.players[].player_role.position_group",
        "$.players[].player_role.name",
        "$.players[].player_role.acronym",
        "$.players[].playing_time.total",
        "$.players[].playing_time.total.minutes_tip",
        "$.players[].playing_time.total.minutes_otip",
        "$.players[].playing_time.total.start_frame",
        "$.players[].playing_time.total.end_frame",
        "$.players[].playing_time.total.minutes_played",
        "$.players[].playing_time.total.minutes_played_regular_time",
        "$.players[].playing_time.by_period[].name",
        "$.players[].playing_time.by_period[].minutes_tip",
        "$.players[].playing_time.by_period[].minutes_otip",
        "$.players[].playing_time.by_period[].start_frame",
        "$.players[].playing_time.by_period[].end_frame",
        "$.players[].playing_time.by_period[].minutes_played",
        "$.players[].playing_time.sequences[].period",
        "$.players[].playing_time.sequences[].start_frame",
        "$.players[].playing_time.sequences[].end_frame",
        # $.referees[] is not here: it is an empty list in all 20 matches, so it carries
        # no scalar leaf to lose (see bronze/football/meta.py's module docstring).
    }
)


def test_match_json_leaves_all_represented(match_ids: list[int]) -> None:
    raw_root = raw_root_for(DATA, SOURCE_ID)
    print(f"\n{'match_id':>8} {'leaves':>7} {'uncovered':>9}")
    all_uncovered: set[str] = set()
    for match_id in match_ids:
        path = raw_root / "data" / "matches" / str(match_id) / f"{match_id}_match.json"
        m = json.loads(path.read_text(encoding="utf-8"))
        leaves = scalar_leaf_paths(m)
        uncovered = leaves - COVERED_LEAF_PATTERNS
        all_uncovered |= uncovered
        print(f"{match_id:>8} {len(leaves):>7} {len(uncovered):>9}")
    if all_uncovered:
        print("UNCOVERED LEAF PATHS:")
        for p in sorted(all_uncovered):
            print(f"  {p}")
    assert all_uncovered == set()
