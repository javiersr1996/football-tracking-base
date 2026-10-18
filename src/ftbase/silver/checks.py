# Rewritten from tracklab@686508a: pipeline/dbt/models/silver/schema.yml and
# pipeline/dbt/macros/test_unique_combination.sql
# Changes: the integrity tests of the football models (key uniqueness, not_null of key
# columns, relationships) written as data and rendered to SQL here instead of dbt generic
# tests; accepted_values and not_null of non-key columns are not ported, except
# frames.is_detected and frames_attacking.attack_sign (added: the analysis reads them
# without a null guard), nor are the column descriptions; the six singular tests are read
# from assertions/*.sql.
"""Silver's checks: every check is a query that returns the offending rows, so 0 rows passes.

Two kinds, run by ``ftbase silver`` after the build and by the tests:

- :data:`SCHEMA_CHECKS`, the integrity half of tracklab's ``schema.yml``: the key of each
  table is unique, its key columns are not null, and every foreign key lands in its
  parent. ``unique`` on one column ignores nulls (dbt's ``unique``); on several columns
  it groups them as they are (tracklab's ``unique_combination``). ``relationships``
  ignores a null child (dbt's semantics: the ball has no team).
- The six singular assertions in ``assertions/*.sql`` (tracklab's dbt tests), each a
  ``SELECT`` that must come back empty.

They read the table and view names :func:`ftbase.silver.read.connect` registers.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

import duckdb

SAMPLE_ROWS = 5


@dataclass(frozen=True)
class Check:
    name: str
    sql: str


@dataclass(frozen=True)
class CheckResult:
    name: str
    n_rows: int
    sample: list[tuple[Any, ...]]

    @property
    def ok(self) -> bool:
        return self.n_rows == 0


def unique(model: str, *columns: str) -> Check:
    cols = ", ".join(columns)
    where = f"WHERE {columns[0]} IS NOT NULL " if len(columns) == 1 else ""
    return Check(
        f"unique {model} ({cols})",
        f"SELECT {cols}, count(*) AS n_rows FROM {model} {where}"
        f"GROUP BY {cols} HAVING count(*) > 1",
    )


def not_null(model: str, column: str) -> Check:
    return Check(f"not_null {model}.{column}", f"SELECT * FROM {model} WHERE {column} IS NULL")


def relationship(model: str, column: str, to: str, field: str) -> Check:
    return Check(
        f"relationships {model}.{column} -> {to}.{field}",
        f"SELECT c.{column} FROM {model} c ANTI JOIN {to} p ON p.{field} = c.{column} "
        f"WHERE c.{column} IS NOT NULL",
    )


def _key(model: str, *columns: str) -> list[Check]:
    """The table's key: unique together, and none of its columns null."""
    return [unique(model, *columns), *(not_null(model, c) for c in columns)]


def _foreign(model: str, column: str, to: str, field: str, nullable: bool = False) -> list[Check]:
    checks = [relationship(model, column, to, field)]
    return checks if nullable else [not_null(model, column), *checks]


SCHEMA_CHECKS: tuple[Check, ...] = (
    *_key("dim_sport", "sport"),
    *_key("dim_sport", "id_prefix"),
    *_key("dim_match", "match_id"),
    *_foreign("dim_match", "sport", "dim_sport", "sport"),
    *_foreign("dim_match", "home_team_id", "dim_team", "team_id"),
    *_foreign("dim_match", "away_team_id", "dim_team", "team_id"),
    *_key("dim_period", "match_id", "period"),
    relationship("dim_period", "match_id", "dim_match", "match_id"),
    *_key("dim_team", "team_id"),
    *_foreign("dim_team", "sport", "dim_sport", "sport"),
    *_key("dim_team_period", "match_id", "period", "team_id"),
    relationship("dim_team_period", "match_id", "dim_match", "match_id"),
    relationship("dim_team_period", "team_id", "dim_team", "team_id"),
    *_key("dim_entity", "match_id", "entity_id"),
    relationship("dim_entity", "match_id", "dim_match", "match_id"),
    *_foreign("dim_entity", "team_id", "dim_team", "team_id", nullable=True),
    *_key("frames", "match_id", "frame_native", "entity_id"),
    *_foreign("frames", "sport", "dim_sport", "sport"),
    relationship("frames", "match_id", "dim_match", "match_id"),
    *_foreign("frames", "team_id", "dim_team", "team_id", nullable=True),
    not_null("frames", "is_detected"),
    not_null("frames_attacking", "attack_sign"),
    *_key("events", "match_id", "event_id"),
    unique("events", "event_id"),
    *_foreign("events", "sport", "dim_sport", "sport"),
    relationship("events", "match_id", "dim_match", "match_id"),
    *_foreign("events", "team_id", "dim_team", "team_id", nullable=True),
    *_key("frame_possession", "match_id", "frame_native"),
    *_foreign("frame_possession", "possession_id", "events", "event_id"),
    *_foreign("frame_possession", "team_id", "dim_team", "team_id"),
    not_null("event_type_map", "sport"),
    not_null("event_type_map", "native_family"),
)


def assertion_checks() -> list[Check]:
    """The singular assertions shipped in ``assertions/``, in file-name order."""
    folder = files("ftbase.silver") / "assertions"
    paths = sorted((p for p in folder.iterdir() if p.name.endswith(".sql")), key=lambda p: p.name)
    return [Check(p.name.removesuffix(".sql"), p.read_text(encoding="utf-8")) for p in paths]


def all_checks() -> list[Check]:
    return [*SCHEMA_CHECKS, *assertion_checks()]


def run_check(con: duckdb.DuckDBPyConnection, check: Check) -> CheckResult:
    query = f"(\n{check.sql}\n)"
    row = con.execute(f"SELECT count(*) FROM {query} AS q").fetchone()
    assert row is not None
    n_rows = int(row[0])
    sample = (
        con.execute(f"SELECT * FROM {query} AS q LIMIT {SAMPLE_ROWS}").fetchall() if n_rows else []
    )
    return CheckResult(check.name, n_rows, sample)


def run_checks(
    con: duckdb.DuckDBPyConnection, checks: Sequence[Check] | None = None
) -> list[CheckResult]:
    return [run_check(con, c) for c in (all_checks() if checks is None else checks)]
