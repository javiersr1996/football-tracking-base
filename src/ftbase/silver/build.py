# Rewritten from tracklab@686508a: pipeline/dbt/models/sources/football.yml and
# pipeline/dbt/models/sources/common.yml
# Changes: the dbt sources, seed loading, materialisations and run order become plain
# DuckDB driven from Python in an explicit order; football only (no basketball sources,
# no orientation_basketball report); the orientation is computed in the same run instead
# of read from bronze/common; the manifest commit is passed in, not read as a source.
"""``ftbase silver``: bronze to silver with DuckDB, in an explicit order, no dbt.

1. Orientation (:mod:`ftbase.silver.orientation`) from ``match.home_team_side``,
   registered as the ``orientation`` table.
2. The dimensions in :data:`~ftbase.silver.read.DIMS` order (each reads only the ones
   before it), each written to ``silver/<dim>/data.parquet``.
3. The facts, one match at a time: the bronze views ``bronze_tracking_*`` and
   ``bronze_dynamic_events``/``bronze_phases_of_play`` are pointed at that match's files
   and ``frames.sql`` and ``events.sql`` are copied to ``silver/<fact>/<match_id>.parquet``.
   One match at a time keeps memory bounded by a match, not by the rows of all matches.
4. The checks (:mod:`ftbase.silver.checks`) on a fresh connection over what was written,
   so they read the files a consumer will read.

Bronze sources are views named ``bronze_<table>`` over
``<data_dir>/bronze/football/<table>/*.parquet``; the seed is ``event_type_map``; the
pinned commit is ``manifest_sources``. The matches are the files in ``bronze/football/match``.
Every file is written to a temporary name and renamed, so an interrupted run never
leaves a truncated file that still opens; the fact directories are emptied first, so a
match no longer in bronze does not linger in silver.
"""

from __future__ import annotations

import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pyarrow as pa

from ftbase.ingest.manifest import load_manifest
from ftbase.silver.checks import CheckResult, run_checks
from ftbase.silver.orientation import football_signs, orientation_table
from ftbase.silver.read import (
    DIMS,
    FACTS,
    SEEDS,
    connect,
    dim_path,
    fact_path,
    literal,
    model_sql,
    seed_path,
    silver_dir,
)

Log = Callable[[str], None]

SOURCE_ID = "opendata-football"
DIM_SOURCES: tuple[str, ...] = ("match", "match_periods", "match_players")
FACT_SOURCES: tuple[str, ...] = (
    "tracking_frames",
    "tracking_objects",
    "dynamic_events",
    "phases_of_play",
)


@dataclass
class SilverResult:
    rows: dict[str, int] = field(default_factory=dict)
    orientation_failures: list[str] = field(default_factory=list)
    checks: list[CheckResult] = field(default_factory=list)
    seconds: float = 0.0

    @property
    def failures(self) -> list[str]:
        failed_checks = [
            f"{c.name}: {c.n_rows} row(s), e.g. {c.sample}" for c in self.checks if not c.ok
        ]
        return [*self.orientation_failures, *failed_checks]


def bronze_match_ids(data_dir: Path, match_id: int | None = None) -> list[int]:
    """The matches in bronze, or just ``match_id`` when given (it must be there)."""
    folder = data_dir / "bronze" / "football" / "match"
    ids = sorted(int(p.stem) for p in folder.glob("*.parquet")) if folder.is_dir() else []
    if match_id is not None:
        ids = [match_id] if match_id in ids else []
    if not ids:
        wanted = "" if match_id is None else f" {match_id}"
        raise FileNotFoundError(
            f"no bronze match{wanted} under {folder}; run `ftbase bronze` first"
        )
    return ids


def _copy(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> int:
    """``COPY (query) TO path`` through a temporary file; returns the rows written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    row = con.execute(
        f"COPY (\n{query}\n) TO {literal(tmp)} (FORMAT parquet, COMPRESSION zstd)"
    ).fetchone()
    os.replace(tmp, path)
    assert row is not None
    return int(row[0])


def _point(con: duckdb.DuckDBPyConnection, data_dir: Path, table: str, file: str) -> None:
    path = data_dir / "bronze" / "football" / table / file
    con.execute(
        f"CREATE OR REPLACE VIEW bronze_{table} AS SELECT * FROM read_parquet({literal(path)})"
    )


def build_silver(
    data_dir: Path, manifest_path: Path, log: Log = print, match_id: int | None = None
) -> SilverResult:
    """Builds every silver table under ``data_dir`` and runs the checks on what it wrote.

    With ``match_id`` (the match of a partial ``ingest --match``) only that match is built,
    even if bronze holds others from an earlier run.

    Never raises on a failed check: the failures are in :attr:`SilverResult.failures`,
    after every file is written, because the files are how the shortfall is inspected.
    """
    started = time.perf_counter()
    result = SilverResult()
    source = load_manifest(manifest_path).source(SOURCE_ID)
    if source.commit is None:
        raise RuntimeError(f"{source.source_id} has no pinned commit in {manifest_path}")
    match_ids = bronze_match_ids(data_dir, match_id)
    log(f"silver: {len(match_ids)} matches from {data_dir / 'bronze' / 'football'}")

    with duckdb.connect() as con:
        con.register(
            "manifest_sources",
            pa.table({"source_id": [source.source_id], "source_commit": [source.commit]}),
        )
        for name in SEEDS:
            con.execute(
                f"CREATE TABLE {name} AS SELECT * FROM "
                f"read_csv({literal(seed_path(name))}, header = true, all_varchar = true)"
            )
        for table in DIM_SOURCES:
            _point(con, data_dir, table, "*.parquet" if match_id is None else f"{match_id}.parquet")

        orientations = [football_signs(con, data_dir, m) for m in match_ids]
        result.orientation_failures = [
            f"orientation {o.match_id}: phases agree {o.phases_agree}/{o.phases} with "
            f"home_team_side"
            for o in orientations
            if not o.ok
        ]
        con.register("orientation", orientation_table(orientations))

        for name in DIMS:
            con.execute(f"CREATE TABLE {name} AS\n{model_sql(name)}")
            result.rows[name] = _copy(con, f"SELECT * FROM {name}", dim_path(data_dir, name))
            log(f"silver {name}: {result.rows[name]} rows")

        for fact in FACTS:
            shutil.rmtree(silver_dir(data_dir) / fact, ignore_errors=True)
            result.rows[fact] = 0
        for match_id in match_ids:
            for table in FACT_SOURCES:
                _point(con, data_dir, table, f"{match_id}.parquet")
            counts = []
            for fact in FACTS:
                n = _copy(con, model_sql(fact), fact_path(data_dir, fact, match_id))
                result.rows[fact] += n
                counts.append(f"{fact}={n}")
            log(f"silver {match_id}: {' '.join(counts)}")

    with connect(data_dir, match_ids) as con:
        result.checks = run_checks(con)
    result.seconds = time.perf_counter() - started
    return result
