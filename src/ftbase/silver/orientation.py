# Rewritten from tracklab@686508a: pipeline/src/tracklab/silver/orientation.py
# Changes: football only. Kept the sign rule, football_signs (unchanged logic and evidence
# text) and the orientation schema; dropped basketball, the player_possession event-frame
# probe, the reports CSV and the docs rendering (measurements, not part of the build); the
# rows go to an in-memory table that dim_team_period reads in the same run instead of
# bronze/common/orientation, and a disagreeing phase is a failure the build reports.
"""Which way each team attacks in each period, from ``match.home_team_side``.

Football tracking and events are both in metres. ``attack_sign`` is +1 when the team
attacks towards +x of the venue (tracking) frame in that period, -1 otherwise:

- the sign comes from ``match.home_team_side[period]`` (``left_to_right`` = home attacks
  towards +x; away gets the opposite sign);
- it is crossed with ``phases_of_play.attacking_side`` per (period, team): the share of
  agreeing phases goes into ``evidence`` and the bar is 100%. A disagreeing phase is
  counted, never voted: the sign still comes from ``home_team_side`` and the match is
  reported as failed (:attr:`MatchOrientation.ok`).

A ``home_team_side`` whose length is not the number of periods, a side outside
:data:`SIDES`, or a phase whose team is neither home nor away fails the run naming the
file, the period and the team.

The rows feed ``dim_team_period`` (``sql/dim_team_period.sql``)::

    sport | native_match_id | period | native_team_id | attack_sign | evidence
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from ftbase.bronze.common import bronze_path

SPORT = "football"
SIDES: tuple[str, ...] = ("left_to_right", "right_to_left")

ORIENTATION_SCHEMA = pa.schema(
    [
        pa.field("sport", pa.string(), nullable=False),
        pa.field("native_match_id", pa.int64(), nullable=False),
        pa.field("period", pa.int16(), nullable=False),
        pa.field("native_team_id", pa.int64(), nullable=False),
        pa.field("attack_sign", pa.int16(), nullable=False),
        pa.field("evidence", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class TeamPeriodSign:
    sport: str
    native_match_id: int
    period: int
    native_team_id: int
    attack_sign: int
    evidence: str


@dataclass(frozen=True)
class MatchOrientation:
    match_id: int
    signs: list[TeamPeriodSign]
    phases_agree: int
    phases: int

    @property
    def ok(self) -> bool:
        """Every phase agrees with ``home_team_side`` (and there is at least one)."""
        return self.phases > 0 and self.phases_agree == self.phases


def attack_sign_from_side(side: str) -> int:
    """``left_to_right`` = the team attacks towards +x of the venue frame."""
    if side == "left_to_right":
        return 1
    if side == "right_to_left":
        return -1
    raise ValueError(f"unknown attacking side {side!r}, expected one of {SIDES}")


def side_from_attack_sign(sign: int) -> str:
    return "left_to_right" if sign > 0 else "right_to_left"


def _parquet(data_dir: Path, table: str, match_id: int) -> str:
    path = bronze_path(data_dir, SPORT, table, match_id)
    if not path.is_file():
        raise FileNotFoundError(f"{path} missing; run `ftbase bronze` first")
    return path.as_posix()


def _one(con: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object]) -> tuple[Any, ...]:
    row = con.execute(sql, list(params)).fetchone()
    assert row is not None
    return row


def football_signs(
    con: duckdb.DuckDBPyConnection, data_dir: Path, match_id: int
) -> MatchOrientation:
    """One sign per (period, team) from ``match.home_team_side``, crossed with the phases.

    Reads ``match``, ``match_periods`` and ``phases_of_play`` of one match from bronze
    under ``data_dir``. A team or side value outside the match's vocabulary fails naming
    the file, period and team.
    """
    match = _parquet(data_dir, "match", match_id)
    periods = _parquet(data_dir, "match_periods", match_id)
    phases = _parquet(data_dir, "phases_of_play", match_id)
    m = _one(con, "SELECT home_team_id, away_team_id, home_team_side FROM read_parquet(?)", [match])
    home, away, sides = int(m[0]), int(m[1]), [str(s) for s in m[2]]
    (n_periods,) = _one(con, "SELECT count(*) FROM read_parquet(?)", [periods])
    if len(sides) != n_periods:
        raise ValueError(
            f"{match}: home_team_side has {len(sides)} entries but match_periods has "
            f"{n_periods} rows"
        )
    for i, side in enumerate(sides, start=1):
        if side not in SIDES:
            raise ValueError(f"{match}: home_team_side[{i}] = {side!r}, expected one of {SIDES}")
    counts: dict[tuple[int, int], dict[str, int]] = {}
    for period, team, side, n in con.execute(
        "SELECT period, team_in_possession_id, attacking_side, count(*) "
        "FROM read_parquet(?) GROUP BY ALL",
        [phases],
    ).fetchall():
        where = f"{phases}: period {period} team {team}"
        if team not in (home, away):
            raise ValueError(
                f"{where}: team_in_possession_id is neither home {home} nor away {away}"
            )
        if side not in SIDES:
            raise ValueError(f"{where}: attacking_side {side!r}, expected one of {SIDES}")
        if not 1 <= period <= n_periods:
            raise ValueError(f"{where}: period outside match_periods (1..{n_periods})")
        counts.setdefault((int(period), int(team)), {})[str(side)] = int(n)
    out: list[TeamPeriodSign] = []
    agree = total = 0
    for period, home_side in enumerate(sides, start=1):
        home_sign = attack_sign_from_side(home_side)
        for label, team, sign in (("home", home, home_sign), ("away", away, -home_sign)):
            expected = side_from_attack_sign(sign)
            c = counts.get((period, team), {})
            n_ok, n_all = c.get(expected, 0), sum(c.values())
            agree += n_ok
            total += n_all
            out.append(
                TeamPeriodSign(
                    SPORT,
                    match_id,
                    period,
                    team,
                    sign,
                    f"match.home_team_side[{period}]={home_side} ({label}); "
                    f"phases_of_play.attacking_side={expected} in {n_ok}/{n_all} phases",
                )
            )
    return MatchOrientation(match_id, out, agree, total)


def orientation_table(results: Sequence[MatchOrientation]) -> pa.Table:
    """The rows of every match as one table of :data:`ORIENTATION_SCHEMA`."""
    rows = sorted(
        (s for r in results for s in r.signs),
        key=lambda s: (s.native_match_id, s.period, s.native_team_id),
    )
    return pa.table(
        {name: [getattr(s, name) for s in rows] for name in ORIENTATION_SCHEMA.names},
        schema=ORIENTATION_SCHEMA,
    )
