# Copied from tracklab@686508a: pipeline/src/tracklab/bronze/common.py
# Changes: imports; ftbase names in docs and messages; Parquet metadata keys ftbase.*;
# basketball mentions dropped.
"""What every bronze table shares: where the raw bytes are, where Parquet goes, how it is written.

Bronze is the provider in Parquet: native names, native rows, no filtering and no
derivation. The only transformations are type casts declared in the schema of each
table and documented next to it. This module fixes the conventions that every bronze
builder (football tracking, events and meta) copies:

- Layout: one file per match and table, ``<data_dir>/bronze/<sport>/<table>/<match_id>.parquet``.
  The match id is also a column of every row, so the path is never load-bearing and
  ``read_parquet('.../<table>/*.parquet')`` reads a whole sport with plain column
  filters (each file holds a single match_id, so row-group statistics prune files).
- Lineage: every table starts with ``sport``, ``match_id``, ``source_file`` (the
  bundle-relative path of the raw file the row was read from) and ``source_row`` (the
  0-based position of the record in that file: line of a JSONL, index in a JSON array,
  data row of a CSV). ``source_row`` is not a frame or event number, and keeping
  both columns makes the difference visible instead of hiding it.
- Typing: a batch is built from named columns against the declared schema. A column
  missing, a column too many, a value that does not cast, or a null in a non-nullable
  field fails the run with the column's name; nothing is coerced in silence.
- Writing: ZSTD, 100k-row row groups, and atomic (temp file then rename) so an
  interrupted run never leaves a truncated file that still opens. The pinned commit
  and the sha256 of the raw file travel in the Parquet key-value metadata.
- Input: bronze reads the raw root that ``ftbase ingest`` verified, resolved from
  the receipt it wrote (``source_path`` when the bytes live in an outside clone).

Exception to lineage: season-grain tables have no match in their grain (one row per
player-team-season or per CSV data row, not per match). Those use ``SEASON_LINEAGE_FIELDS``
(``sport, source_file, source_row``, no ``match_id``) and live at
``bronze_season_path()`` instead of ``bronze_path()``.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ftbase.formats.csvschema import FrozenSchema, pa_schema, read_typed
from ftbase.ingest.fetch import RECEIPT_NAME
from ftbase.ingest.manifest import FileEntry

ROW_GROUP_ROWS = 100_000
COMPRESSION = "zstd"

LINEAGE_FIELDS: tuple[pa.Field, ...] = (
    pa.field("sport", pa.string(), nullable=False),
    pa.field("match_id", pa.int64(), nullable=False),
    pa.field("source_file", pa.string(), nullable=False),
    pa.field("source_row", pa.int64(), nullable=False),
)

# For the season-grain tables that carry no match_id (see the module docstring).
SEASON_LINEAGE_FIELDS: tuple[pa.Field, ...] = (
    pa.field("sport", pa.string(), nullable=False),
    pa.field("source_file", pa.string(), nullable=False),
    pa.field("source_row", pa.int64(), nullable=False),
)


def raw_root_for(data_dir: Path, source_id: str) -> Path:
    """Directory holding the verified raw bundle of ``source_id`` (its ``data/`` is inside).

    Read from the receipt ``ftbase ingest`` wrote: ``source_path`` when the bytes
    were verified in place inside an outside clone, else the receipt's own directory.
    A missing receipt or a receipt with failures means nothing verified is on disk.
    """
    receipt_dir = data_dir / "raw" / source_id
    receipt_path = receipt_dir / RECEIPT_NAME
    if not receipt_path.is_file():
        raise FileNotFoundError(f"{receipt_path} not found; run `ftbase ingest` first")
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if receipt["failed"]:
        raise RuntimeError(
            f"{source_id}: {len(receipt['failed'])} file(s) failed verification; "
            f"re-run `ftbase ingest`"
        )
    source_path = receipt.get("source_path")
    return Path(source_path) if source_path else receipt_dir


def bronze_path(data_dir: Path, sport: str, table: str, match_id: int) -> Path:
    return data_dir / "bronze" / sport / table / f"{match_id}.parquet"


def bronze_season_path(data_dir: Path, sport: str, table: str) -> Path:
    """Path of a season-grain table (no match_id): one file for the whole bundle."""
    return data_dir / "bronze" / sport / table / "data.parquet"


def batch_from_columns(
    schema: pa.Schema, columns: Mapping[str, Sequence[Any] | Any], n_rows: int
) -> pa.RecordBatch:
    """A RecordBatch of ``schema`` from named columns, failing with the column's name.

    A pyarrow array is taken as built (a fixed-size list column); any other value that is not a
    list is a per-batch constant (the lineage columns) and is repeated ``n_rows`` times.
    """
    missing = [f.name for f in schema if f.name not in columns]
    extra = sorted(set(columns) - set(schema.names))
    if missing or extra:
        raise ValueError(f"columns missing {missing}, unexpected {extra}")
    arrays: list[pa.Array] = []
    for field in schema:
        values = columns[field.name]
        try:
            if isinstance(values, list):
                array = pa.array(values, type=field.type)
            elif isinstance(values, pa.Array):  # built by the caller (a fixed-size list column)
                array = values.cast(field.type)
            else:
                array = pa.repeat(pa.scalar(values, type=field.type), n_rows)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError, TypeError) as exc:
            raise ValueError(f"column {field.name}: {exc}") from exc
        if len(array) != n_rows:
            raise ValueError(f"column {field.name}: {len(array)} values for {n_rows} rows")
        if not field.nullable and array.null_count:
            raise ValueError(f"column {field.name}: {array.null_count} null(s), declared NOT NULL")
        arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


class ParquetSink:
    """Writes batches of one declared schema to one Parquet file, atomically.

    The file only appears under its final name when the ``with`` block exits
    cleanly; on an exception the partial temp file is removed. ``rows`` and
    ``bytes`` are filled on exit.
    """

    def __init__(self, path: Path, schema: pa.Schema, metadata: Mapping[str, str]) -> None:
        self.path = path
        self.rows = 0
        self.bytes = 0
        self._tmp = path.with_name(path.name + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(
            self._tmp, schema.with_metadata(dict(metadata)), compression=COMPRESSION
        )

    def __enter__(self) -> ParquetSink:
        return self

    def write(self, batch: pa.RecordBatch) -> None:
        if batch.num_rows == 0:
            return
        self._writer.write_batch(batch, row_group_size=ROW_GROUP_ROWS)
        self.rows += batch.num_rows

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._writer.close()
        if exc_type is not None:
            self._tmp.unlink(missing_ok=True)
            return
        os.replace(self._tmp, self.path)
        self.bytes = self.path.stat().st_size


@dataclass(frozen=True)
class SeasonCsvStats:
    table: str
    rows: int
    seconds: float
    mb: float


def season_csv_schema(frozen: FrozenSchema) -> pa.Schema:
    """``SEASON_LINEAGE_FIELDS`` plus every frozen column of a season CSV family."""
    return pa.schema([*SEASON_LINEAGE_FIELDS, *pa_schema(frozen)])


def build_season_csv_table(
    sport: str,
    table: str,
    frozen: FrozenSchema,
    entry: FileEntry,
    raw_root: Path,
    commit: str,
    data_dir: Path,
    con: duckdb.DuckDBPyConnection,
) -> SeasonCsvStats:
    """One frozen, season-grain CSV (no match dimension) to Parquet with
    ``SEASON_LINEAGE_FIELDS``. Used by season-grain CSV families (the
    football aggregates are not part of ftbase yet): bronze does not
    sniff the file, it loads the ``*.schema.json`` the sport's ``freeze-schema``
    already froze and imposes it with ``formats.csvschema.read_typed`` (DuckDB
    ``read_csv(columns=...)``, so a value that does not parse fails here with the
    file name). Each file is at most a few hundred rows, so it is read whole into
    pyarrow, no streaming.
    """
    started = time.perf_counter()
    schema = season_csv_schema(frozen)
    data = read_typed(frozen, raw_root / entry.path, con)
    n = data.num_rows
    columns: dict[str, Any] = {
        "sport": sport,
        "source_file": entry.path,
        "source_row": list(range(n)),
        **{name: data.column(name).to_pylist() for name in data.column_names},
    }
    batch = batch_from_columns(schema, columns, n)
    metadata = {
        "ftbase.sport": sport,
        "ftbase.source_file": entry.path,
        "ftbase.source_sha256": entry.sha256,
        "ftbase.source_commit": commit,
    }
    out_path = bronze_season_path(data_dir, sport, table)
    with ParquetSink(out_path, schema, metadata) as sink:
        sink.write(batch)
    return SeasonCsvStats(table, sink.rows, time.perf_counter() - started, sink.bytes / 1e6)
