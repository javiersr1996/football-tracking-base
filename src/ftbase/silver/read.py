# New in ftbase (no tracklab origin): tracklab read silver through the views dbt left in
# its DuckDB file; ftbase registers them per connection and exposes typed readers.
"""Where silver lives on disk and how it is read back.

Layout under ``<data_dir>/silver/``:

- one file per dimension, ``<dim>/data.parquet`` (:data:`DIMS`);
- one file per match for the facts, ``frames/<match_id>.parquet`` and
  ``events/<match_id>.parquet`` (:data:`FACTS`), like bronze;
- the two views (:data:`VIEWS`) are not written: :func:`connect` defines them from
  ``sql/<view>.sql`` on every connection.

:func:`read_frames`, :func:`read_events` and :func:`read_dim` are the public readers.
The fact readers take the match ids explicitly and open only those matches' files, so a
caller that must keep some matches unread can rely on it.
"""

from __future__ import annotations

from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path
from typing import Literal

import duckdb
import pyarrow as pa

from ftbase.paths import data_root

DIMS: tuple[str, ...] = (
    "dim_sport",
    "dim_period",
    "dim_team",
    "dim_match",
    "dim_team_period",
    "dim_entity",
)
FACTS: tuple[str, ...] = ("frames", "events")
VIEWS: tuple[str, ...] = ("frame_possession", "frames_attacking")
SEEDS: tuple[str, ...] = ("event_type_map",)

DimName = Literal[
    "dim_sport", "dim_period", "dim_team", "dim_match", "dim_team_period", "dim_entity"
]

FRAMES_ORDER = ("native_match_id", "frame_native", "entity_id")
EVENTS_ORDER = ("native_match_id", "period", "frame_start", "event_id")


def model_sql(name: str) -> str:
    """The SELECT of a silver model or view, as shipped in ``ftbase/silver/sql/``."""
    return (files("ftbase.silver") / "sql" / f"{name}.sql").read_text(encoding="utf-8")


def seed_path(name: str) -> Path:
    return Path(str(files("ftbase.silver") / "seeds" / f"{name}.csv"))


def silver_dir(data_dir: Path) -> Path:
    return data_dir / "silver"


def dim_path(data_dir: Path, name: str) -> Path:
    return silver_dir(data_dir) / name / "data.parquet"


def fact_path(data_dir: Path, name: str, match_id: int) -> Path:
    return silver_dir(data_dir) / name / f"{match_id}.parquet"


def literal(path: Path) -> str:
    """A path as a SQL string literal (views cannot take bound parameters)."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def built_match_ids(data_dir: Path) -> list[int]:
    """The matches whose frames silver holds, from the file names."""
    folder = silver_dir(data_dir) / "frames"
    return sorted(int(p.stem) for p in folder.glob("*.parquet")) if folder.is_dir() else []


def connect(
    data_dir: Path | None = None, match_ids: Sequence[int] | None = None
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection with silver registered as views.

    Every dimension and the seed are views over their files; ``frames`` and ``events``
    are views over the files of ``match_ids`` only (every built match when None);
    ``frame_possession`` and ``frames_attacking`` are defined on top. A missing file
    raises :class:`FileNotFoundError` naming it.
    """
    data_dir = data_root() if data_dir is None else data_dir
    ids = built_match_ids(data_dir) if match_ids is None else list(match_ids)
    if not ids:
        raise ValueError(f"no match to read under {silver_dir(data_dir)}; run `ftbase silver`")
    paths = {name: dim_path(data_dir, name) for name in DIMS}
    for fact in FACTS:
        for match_id in ids:
            paths[f"{fact} {match_id}"] = fact_path(data_dir, fact, match_id)
    missing = [p.as_posix() for p in paths.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError(f"silver files missing ({len(missing)}): {missing[:5]}")
    con = duckdb.connect()
    for name in DIMS:
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({literal(paths[name])})")
    for name in SEEDS:
        con.execute(
            f"CREATE VIEW {name} AS SELECT * FROM "
            f"read_csv({literal(seed_path(name))}, header = true, all_varchar = true)"
        )
    for fact in FACTS:
        listed = ", ".join(literal(paths[f"{fact} {m}"]) for m in ids)
        con.execute(f"CREATE VIEW {fact} AS SELECT * FROM read_parquet([{listed}])")
    for view in VIEWS:
        con.execute(f"CREATE VIEW {view} AS {model_sql(view)}")
    return con


def _read(
    table: str,
    match_ids: Sequence[int],
    columns: Sequence[str] | None,
    order: Sequence[str],
    data_dir: Path | None,
) -> pa.Table:
    ids = sorted({int(m) for m in match_ids})
    if not ids:
        raise ValueError("name at least one match id")
    with connect(data_dir, ids) as con:
        known = [str(r[0]) for r in con.execute(f"DESCRIBE {table}").fetchall()]
        wanted = known if columns is None else list(columns)
        unknown = [c for c in wanted if c not in known]
        if unknown:
            raise ValueError(f"{table} has no column(s) {unknown}; it has {known}")
        select = ", ".join(f'"{c}"' for c in wanted)
        return con.sql(f"SELECT {select} FROM {table} ORDER BY {', '.join(order)}").to_arrow_table()


def read_frames(
    match_ids: Sequence[int],
    *,
    columns: Sequence[str] | None = None,
    attacking: bool = False,
    data_dir: Path | None = None,
) -> pa.Table:
    """Silver frames of the given matches, as a pyarrow Table.

    Contract (one row per match, native frame and entity, frames outside every period
    excluded; an entity not observed in a frame has no row):

    ========================  ========  ==================================================
    column                    type      meaning
    ========================  ========  ==================================================
    ``sport``                 string    always ``football``
    ``match_id``              string    ``fb:<native_match_id>``
    ``native_match_id``       int64     SkillCorner match id
    ``period``                int16     1 or 2, never null
    ``frame_native``          int64     the provider's frame number (10 Hz)
    ``t_ms``                  int64     ``frame_native * 100``
    ``clock_s``               double    seconds since the period's first frame
    ``entity_id``             string    ``fb:<player id>`` or ``fb:ball``
    ``entity_kind``           string    ``player`` or ``ball``
    ``team_id``               string    ``fb:<team id>``; null for the ball
    ``x_m``, ``y_m``          double    metres, in the venue (tracking) frame as delivered
    ``z_m``                   double    the ball's height as delivered; null for players
    ``is_detected``           bool      false: the provider extrapolated the position
    ========================  ========  ==================================================

    With ``attacking=True`` the rows come from ``frames_attacking`` instead: only frames
    inside a team possession, with ``possession_id``, ``team_in_possession_id``,
    ``attack_sign`` (int16, +1 or -1) and ``x_att_m``/``y_att_m`` (the venue position
    times the sign of the team in possession, so that team attacks towards +x).

    Rows are ordered by ``native_match_id, frame_native, entity_id``. ``columns`` picks
    and orders the columns (unknown names raise :class:`ValueError`). Only the files of
    ``match_ids`` are opened; a match silver does not hold raises
    :class:`FileNotFoundError`. ``data_dir`` defaults to ``$FTBASE_DATA_ROOT`` or ``data``.
    """
    table = "frames_attacking" if attacking else "frames"
    return _read(table, match_ids, columns, FRAMES_ORDER, data_dir)


def read_events(
    match_ids: Sequence[int],
    *,
    columns: Sequence[str] | None = None,
    data_dir: Path | None = None,
) -> pa.Table:
    """Silver events of the given matches, as a pyarrow Table.

    One row per event: the four native families of ``dynamic_events``, the phases of
    play (``native_family = 'phases_of_play'``) and the team possessions derived by the
    chain rule (``event_type = 'possession'``). Keys: ``match_id``, ``event_id`` (unique),
    ``native_family`` and ``native_event_id`` (the join back to bronze). Coordinates
    (``x_start_m``, ``y_start_m``, ``x_end_m``, ``y_end_m``) are rotated to the venue
    frame of :func:`read_frames`. ``possession_id`` is the ``event_id`` of the team
    possession the event starts in, null between possessions. Rows are ordered by
    ``native_match_id, period, frame_start, event_id``; ``columns``, ``match_ids`` and
    ``data_dir`` behave as in :func:`read_frames`.
    """
    return _read("events", match_ids, columns, EVENTS_ORDER, data_dir)


def read_dim(name: DimName, *, data_dir: Path | None = None) -> pa.Table:
    """One silver dimension, whole (they are small: at most a few hundred rows a match)."""
    if name not in DIMS:
        raise ValueError(f"unknown dimension {name!r}, expected one of {DIMS}")
    path = dim_path(data_root() if data_dir is None else data_dir, name)
    if not path.is_file():
        raise FileNotFoundError(f"{path} missing; run `ftbase silver`")
    with duckdb.connect() as con:
        return con.sql(f"SELECT * FROM read_parquet({literal(path)})").to_arrow_table()
