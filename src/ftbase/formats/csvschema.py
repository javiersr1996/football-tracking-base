# Copied from tracklab@686508a: pipeline/src/tracklab/formats/csvschema.py
# Changes: none (header only).
"""Freeze the schema of a family of CSV files into a `*.schema.json` that bronze imposes.

DuckDB sniffs every file of the family over all its rows. Per column, the votes of
files where the column is entirely null are discarded (DuckDB types an empty column
as VARCHAR, which says nothing), the remaining votes are promoted along
BOOLEAN < BIGINT < DOUBLE < VARCHAR, and anything else that disagrees falls back to
VARCHAR. The frozen schema is then re-imposed on every file with `read_csv(columns=...)`
so a type that does not parse fails here, at freeze time, with the file name.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa

_PROMOTION = {"BOOLEAN": 0, "BIGINT": 1, "DOUBLE": 2, "VARCHAR": 3}
_PA_TYPES: dict[str, pa.DataType] = {
    "BOOLEAN": pa.bool_(),
    "BIGINT": pa.int64(),
    "DOUBLE": pa.float64(),
    "VARCHAR": pa.string(),
    "DATE": pa.date32(),
}
# Dialect stated explicitly: giving `columns` to read_csv turns auto-detection off.
_DIALECT = "header=true, delim=',', quote='\"', escape='\"'"


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class FrozenSchema:
    table: str
    header_sha256: str
    columns: tuple[Column, ...]
    all_null_columns: tuple[str, ...]
    frozen_from: tuple[str, ...]

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def duckdb_columns(self) -> dict[str, str]:
        return {c.name: c.type for c in self.columns}

    def to_json(self, **extra: object) -> str:
        payload: dict[str, object] = {
            "table": self.table,
            **extra,
            "n_columns": len(self.columns),
            "header_sha256": self.header_sha256,
            "columns": [{"name": c.name, "type": c.type} for c in self.columns],
            "all_null_columns": list(self.all_null_columns),
            "frozen_from": list(self.frozen_from),
        }
        return json.dumps(payload, indent=2) + "\n"


def header_line(path: Path) -> str:
    with path.open(encoding="utf-8", newline="") as fh:
        return fh.readline().rstrip("\r\n")


def header_sha256(path: Path) -> str:
    return hashlib.sha256(header_line(path).encode("utf-8")).hexdigest()


def header_names(path: Path) -> list[str]:
    # Header cells are bare identifiers in every family of this bundle; no quoting.
    return header_line(path).split(",")


def load_schema(path: Path) -> FrozenSchema:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return FrozenSchema(
        table=raw["table"],
        header_sha256=raw["header_sha256"],
        columns=tuple(Column(c["name"], c["type"]) for c in raw["columns"]),
        all_null_columns=tuple(raw["all_null_columns"]),
        frozen_from=tuple(raw["frozen_from"]),
    )


def _sniff(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str, int]]:
    """(name, sniffed type, non-null count) per column, over every row of the file."""
    described = con.execute(
        "DESCRIBE SELECT * FROM read_csv(?, header=true, sample_size=-1)", [str(path)]
    ).fetchall()
    names = [row[0] for row in described]
    types = [row[1] for row in described]
    counts_sql = ", ".join(f'count("{name}")' for name in names)
    counts = con.execute(
        f"SELECT {counts_sql} FROM read_csv(?, header=true, all_varchar=true)", [str(path)]
    ).fetchone()
    assert counts is not None
    return list(zip(names, types, counts, strict=True))


def _promote(a: str, b: str) -> str:
    if a == b:
        return a
    if a in _PROMOTION and b in _PROMOTION:
        return a if _PROMOTION[a] > _PROMOTION[b] else b
    return "VARCHAR"


def freeze_csv_family(
    table: str, paths: list[Path], relative_to: Path, con: duckdb.DuckDBPyConnection
) -> FrozenSchema:
    """Sniff, merge and re-impose. Raises if the headers differ or a type does not parse."""
    if not paths:
        raise ValueError(f"{table}: no files to freeze")
    hashes = {header_sha256(p) for p in paths}
    if len(hashes) != 1:
        raise ValueError(f"{table}: headers differ across {len(paths)} files: {sorted(hashes)}")
    names = header_names(paths[0])

    merged: dict[str, str | None] = dict.fromkeys(names)
    for path in paths:
        for name, sniffed, non_null in _sniff(con, path):
            if non_null == 0:
                continue
            current = merged[name]
            merged[name] = sniffed if current is None else _promote(current, sniffed)

    columns = tuple(Column(name, merged[name] or "VARCHAR") for name in names)
    schema = FrozenSchema(
        table=table,
        header_sha256=hashes.pop(),
        columns=columns,
        all_null_columns=tuple(name for name in names if merged[name] is None),
        frozen_from=tuple(p.relative_to(relative_to).as_posix() for p in paths),
    )
    for path in paths:
        impose(schema, path, con)
    return schema


def impose(schema: FrozenSchema, path: Path, con: duckdb.DuckDBPyConnection) -> int:
    """Read `path` with the frozen types and return its row count. Raises on any mismatch."""
    if header_names(path) != list(schema.names):
        raise ValueError(f"{path.name}: header does not match frozen {schema.table}")
    row = con.execute(
        f"SELECT count(*) FROM read_csv(?, columns=?, {_DIALECT})",
        [str(path), schema.duckdb_columns()],
    ).fetchone()
    assert row is not None
    return int(row[0])


def pa_schema(schema: FrozenSchema) -> pa.Schema:
    """The pyarrow schema of a frozen CSV family, all columns nullable: sniffing a
    file's type says nothing about whether a column is ever null in it, so the caller
    (bronze) cannot declare NOT NULL from this alone. It prepends its own lineage.

    A frozen type this module does not know how to store fails naming the table,
    the column and the type, instead of a bare `KeyError` that sends the caller
    hunting through the schema file to find which column it was.
    """
    fields = []
    for c in schema.columns:
        try:
            pa_type = _PA_TYPES[c.type]
        except KeyError as exc:
            raise ValueError(f"{schema.table}.{c.name}: no pyarrow type for {c.type!r}") from exc
        fields.append(pa.field(c.name, pa_type, nullable=True))
    return pa.schema(fields)


def read_typed(schema: FrozenSchema, path: Path, con: duckdb.DuckDBPyConnection) -> pa.Table:
    """`impose`, but returning the data: the whole file, typed to the frozen columns.

    For bronze tables built from a handful of season-sized CSVs (the basketball
    aggregates, and the football aggregates that will reuse this), the frozen columns
    are few enough rows and columns that reading the file whole into pyarrow is
    simpler than streaming it, and the caller casts and adds lineage from there.
    """
    if header_names(path) != list(schema.names):
        raise ValueError(f"{path.name}: header does not match frozen {schema.table}")
    return con.execute(
        f"SELECT * FROM read_csv(?, columns=?, {_DIALECT})",
        [str(path), schema.duckdb_columns()],
    ).to_arrow_table()
