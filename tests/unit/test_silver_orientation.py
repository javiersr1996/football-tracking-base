# Rewritten from tracklab@686508a: pipeline/tests/unit/test_silver_orientation.py
# Changes: football only (the basketball, shot-probe, court and event-frame tests and the
# CLI/report/doc tests dropped); football_signs returns a MatchOrientation; added the
# home_team_side length check and the orientation table schema.
"""silver.orientation on a synthetic bronze in ``tmp_path``: no real data, no network.

One football match (two periods, home 7 and away 8) is written as bronze-shaped Parquet
with only the columns the orientation reads. ``home_team_side`` is
``[right_to_left, left_to_right]``, so home attacks towards -x in period 1 and +x in
period 2, and the six planted phases all agree with it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ftbase.bronze.common import bronze_path
from ftbase.silver.orientation import (
    ORIENTATION_SCHEMA,
    attack_sign_from_side,
    football_signs,
    orientation_table,
)

FB_MATCH, FB_HOME, FB_AWAY = 800001, 7, 8

Columns = dict[str, tuple[pa.DataType, list[Any]]]


def _write(data_dir: Path, table: str, columns: Columns) -> None:
    path = bronze_path(data_dir, "football", table, FB_MATCH)
    path.parent.mkdir(parents=True, exist_ok=True)
    table_ = pa.table({name: pa.array(values, type=t) for name, (t, values) in columns.items()})
    pq.write_table(table_, path)


def write_football(
    data_dir: Path,
    phases: Columns | None = None,
    sides: list[str] | None = None,
) -> None:
    _write(
        data_dir,
        "match",
        {
            "home_team_id": (pa.int64(), [FB_HOME]),
            "away_team_id": (pa.int64(), [FB_AWAY]),
            "home_team_side": (
                pa.list_(pa.string()),
                [sides or ["right_to_left", "left_to_right"]],
            ),
        },
    )
    _write(data_dir, "match_periods", {"period": (pa.int64(), [1, 2])})
    _write(
        data_dir,
        "phases_of_play",
        phases
        or {
            "period": (pa.int64(), [1, 1, 1, 2, 2, 2]),
            "team_in_possession_id": (
                pa.int64(),
                [FB_HOME, FB_HOME, FB_AWAY, FB_HOME, FB_AWAY, FB_AWAY],
            ),
            "attacking_side": (
                pa.string(),
                [
                    "right_to_left",
                    "right_to_left",
                    "left_to_right",
                    "left_to_right",
                    "right_to_left",
                    "right_to_left",
                ],
            ),
        },
    )


def test_attack_sign_rule() -> None:
    assert attack_sign_from_side("left_to_right") == 1
    assert attack_sign_from_side("right_to_left") == -1
    with pytest.raises(ValueError, match="unknown attacking side 'up'"):
        attack_sign_from_side("up")


def test_football_signs_from_home_team_side_crossed_with_phases(tmp_path: Path) -> None:
    write_football(tmp_path)
    result = football_signs(duckdb.connect(), tmp_path, FB_MATCH)
    got = {(s.period, s.native_team_id): s.attack_sign for s in result.signs}
    assert got == {(1, FB_HOME): -1, (1, FB_AWAY): 1, (2, FB_HOME): 1, (2, FB_AWAY): -1}
    assert (result.phases_agree, result.phases) == (6, 6)
    assert result.ok
    by_key = {(s.period, s.native_team_id): s.evidence for s in result.signs}
    assert by_key[(1, FB_HOME)] == (
        "match.home_team_side[1]=right_to_left (home); "
        "phases_of_play.attacking_side=right_to_left in 2/2 phases"
    )
    assert by_key[(2, FB_AWAY)] == (
        "match.home_team_side[2]=left_to_right (away); "
        "phases_of_play.attacking_side=right_to_left in 2/2 phases"
    )


def test_football_signs_count_a_disagreeing_phase(tmp_path: Path) -> None:
    write_football(
        tmp_path,
        phases={
            "period": (pa.int64(), [1, 1, 2]),
            "team_in_possession_id": (pa.int64(), [FB_HOME, FB_HOME, FB_AWAY]),
            "attacking_side": (pa.string(), ["right_to_left", "left_to_right", "right_to_left"]),
        },
    )
    result = football_signs(duckdb.connect(), tmp_path, FB_MATCH)
    assert (result.phases_agree, result.phases) == (2, 3)
    assert not result.ok
    assert {
        s.attack_sign for s in result.signs if s.period == 1 and s.native_team_id == FB_HOME
    } == {-1}, "the sign still comes from home_team_side; the phase is counted, not voted"


def test_football_signs_reject_a_team_outside_the_match(tmp_path: Path) -> None:
    write_football(
        tmp_path,
        phases={
            "period": (pa.int64(), [1]),
            "team_in_possession_id": (pa.int64(), [99]),
            "attacking_side": (pa.string(), ["right_to_left"]),
        },
    )
    with pytest.raises(ValueError, match=r"period 1 team 99: team_in_possession_id is neither"):
        football_signs(duckdb.connect(), tmp_path, FB_MATCH)


def test_football_signs_reject_an_unknown_side_value(tmp_path: Path) -> None:
    write_football(
        tmp_path,
        phases={
            "period": (pa.int64(), [1]),
            "team_in_possession_id": (pa.int64(), [FB_HOME]),
            "attacking_side": (pa.string(), ["sideways"]),
        },
    )
    with pytest.raises(ValueError, match=r"attacking_side 'sideways'"):
        football_signs(duckdb.connect(), tmp_path, FB_MATCH)


def test_football_signs_reject_a_side_per_period_mismatch(tmp_path: Path) -> None:
    write_football(tmp_path, sides=["right_to_left"])
    with pytest.raises(ValueError, match=r"home_team_side has 1 entries but match_periods has 2"):
        football_signs(duckdb.connect(), tmp_path, FB_MATCH)


def test_football_signs_name_the_missing_bronze_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"run `ftbase bronze` first"):
        football_signs(duckdb.connect(), tmp_path, FB_MATCH)


def test_orientation_table_has_the_declared_schema_and_sorted_rows(tmp_path: Path) -> None:
    write_football(tmp_path)
    table = orientation_table([football_signs(duckdb.connect(), tmp_path, FB_MATCH)])
    assert table.schema.equals(ORIENTATION_SCHEMA)
    rows = table.to_pylist()
    assert [(r["period"], r["native_team_id"]) for r in rows] == [
        (1, FB_HOME),
        (1, FB_AWAY),
        (2, FB_HOME),
        (2, FB_AWAY),
    ]
    assert {r["sport"] for r in rows} == {"football"}
