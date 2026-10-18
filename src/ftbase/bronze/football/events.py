# Copied from tracklab@686508a: pipeline/src/tracklab/bronze/football/events.py
# Changes: imports; Parquet metadata keys ftbase.*; docstring no longer cites tracklab commands or
# basketball; error for a missing pinned commit; comments no longer cite internal task numbers or
# docs/formats/football.md.
"""Bronze of the football event CSVs: ``dynamic_events`` and ``phases_of_play``.

Both are per-match CSVs whose 322 and 44 columns were frozen once, in tracklab
(``bronze/schemas/football/{dynamic_events,phases_of_play}.schema.json``). Bronze never
sniffs them again: it loads the frozen schema and imposes it with
``formats.csvschema.read_typed`` (DuckDB ``read_csv(columns=...)``), so a column added,
removed or retyped since the freeze fails the run naming the file. Each file is at most
a few thousand rows, so it is read whole into pyarrow, no streaming.

One thing this module resolves that no other bronze table has needed: **both CSVs
carry a native column literally named ``match_id``**. Measured with DuckDB over the 20
files of each table: the native ``match_id`` equals the file's own match id in every
row, 0 mismatches in ``dynamic_events`` and in ``phases_of_play``.
Since ``bronze.common.LINEAGE_FIELDS`` already reserves ``match_id`` (BIGINT NOT
NULL) for every table, and a pyarrow schema cannot usefully carry two fields of the
same name (``column("match_id")`` turns ambiguous, DuckDB and the contract tests
would break), the native column is folded into the lineage one: its value is
checked row by row against the file's match id, failing loudly and naming the file
and row on any future disagreement, and it is not written as a second column. Every
other native column is kept verbatim, native name, native position. Net columns:
``dynamic_events`` 4 lineage + 321 native (322 minus the folded ``match_id``) = 325;
``phases_of_play`` 4 + 43 = 47.

Nothing here drops a row. Match 1953632's `player_id` 973077 has 35
``dynamic_events`` rows and is not a foreign key bronze enforces
(the id is in no ``players[]`` of ``match.json`` and in no tracking frame); those rows are
loaded like any other.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from ftbase.bronze.common import LINEAGE_FIELDS, ParquetSink, batch_from_columns, bronze_path
from ftbase.formats.csvschema import FrozenSchema, load_schema, pa_schema, read_typed
from ftbase.ingest.manifest import FileEntry, Source

SPORT = "football"
SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas" / "football"
Log = Callable[[str], None]

# table name -> manifest role (ftbase.ingest.manifest._ROLE_PATTERNS).
_TABLE_ROLES = {"dynamic_events": "events", "phases_of_play": "phases"}
# Folded into the lineage `match_id` column; see the module docstring.
_NATIVE_MATCH_ID_COLUMN = "match_id"


@dataclass(frozen=True)
class EventsStats:
    match_id: int
    table: str
    rows: int
    seconds: float
    mb: float


def load_frozen_tables(schemas_dir: Path = SCHEMAS_DIR) -> dict[str, FrozenSchema]:
    return {table: load_schema(schemas_dir / f"{table}.schema.json") for table in _TABLE_ROLES}


def table_schema(frozen: FrozenSchema) -> pa.Schema:
    """Lineage plus every frozen column except the native ``match_id`` (folded)."""
    return pa.schema(
        [*LINEAGE_FIELDS, *(f for f in pa_schema(frozen) if f.name != _NATIVE_MATCH_ID_COLUMN)]
    )


def build_table_match(
    table: str,
    frozen: FrozenSchema,
    schema: pa.Schema,
    entry: FileEntry,
    raw_root: Path,
    commit: str,
    data_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> EventsStats:
    """Read ``entry``'s CSV whole, typed to ``frozen``, and write one Parquet."""
    assert entry.match_id is not None
    started = time.perf_counter()
    data = read_typed(frozen, raw_root / entry.path, con)
    n = data.num_rows
    native_match_id = data.column(_NATIVE_MATCH_ID_COLUMN).to_pylist()
    for i, value in enumerate(native_match_id):
        if value != entry.match_id:
            raise ValueError(
                f"{entry.path} row {i}: match_id {value} != {entry.match_id} (from the filename)"
            )
    columns: dict[str, Any] = {
        "sport": SPORT,
        "match_id": entry.match_id,
        "source_file": entry.path,
        "source_row": list(range(n)),
        **{
            name: data.column(name).to_pylist()
            for name in data.column_names
            if name != _NATIVE_MATCH_ID_COLUMN
        },
    }
    batch = batch_from_columns(schema, columns, n)
    metadata = {
        "ftbase.sport": SPORT,
        "ftbase.source_file": entry.path,
        "ftbase.source_sha256": entry.sha256,
        "ftbase.source_commit": commit,
    }
    out_path = bronze_path(data_dir, SPORT, table, entry.match_id)
    with ParquetSink(out_path, schema, metadata) as sink:
        sink.write(batch)
    return EventsStats(
        entry.match_id, table, sink.rows, time.perf_counter() - started, sink.bytes / 1e6
    )


def build_events(
    source: Source, raw_root: Path, data_dir: Path, log: Log = print
) -> list[EventsStats]:
    """``dynamic_events`` then ``phases_of_play`` for every match, in match id order."""
    commit = source.commit
    if commit is None:
        raise RuntimeError(f"{source.source_id} has no pinned commit in the manifest")
    frozen = load_frozen_tables()
    schemas = {table: table_schema(f) for table, f in frozen.items()}
    entries_by_table = {
        table: {f.match_id: f for f in source.files if f.role == role and f.match_id is not None}
        for table, role in _TABLE_ROLES.items()
    }
    match_ids = sorted(entries_by_table["dynamic_events"])
    for table, entries in entries_by_table.items():
        if sorted(entries) != match_ids:
            raise ValueError(f"{table}: match ids {sorted(entries)} != {match_ids}")
    con = duckdb.connect()
    log(f"{'match_id':>8} {'table':>15} {'rows':>6} {'seconds':>8} {'MB':>7}")
    stats: list[EventsStats] = []
    for match_id in match_ids:
        for table in _TABLE_ROLES:
            entry = entries_by_table[table][match_id]
            s = build_table_match(
                table, frozen[table], schemas[table], entry, raw_root, commit, data_dir, con
            )
            log(f"{s.match_id:>8} {s.table:>15} {s.rows:>6} {s.seconds:>8.2f} {s.mb:>7.2f}")
            stats.append(s)
    return stats
