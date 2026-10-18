"""Every frozen schema on disk must translate to pyarrow, and its header must be the frozen one.

`formats.csvschema.pa_schema` once mapped four DuckDB types until the football season
aggregates hit `player_birthdate`, a DATE; the type had been frozen long before and
nothing was wrong with it. The gap was that no test iterated the schemas that exist.

This module iterates the schema files on disk rather than a list written by hand, so a
type frozen tomorrow fails here, with the file that carries it, instead of half-way
through a build. It also pins each schema's `header_sha256`: the hash of the CSV header
line the schema was frozen from, which must equal the hash of its own column names.
"""

from __future__ import annotations

import datetime
import hashlib
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ftbase.formats.csvschema import (
    Column,
    FrozenSchema,
    freeze_csv_family,
    load_schema,
    read_typed,
)
from ftbase.formats.csvschema import pa_schema as csv_pa_schema

SCHEMAS = Path(__file__).resolve().parents[2] / "src" / "ftbase" / "bronze" / "schemas"
FROZEN = sorted(SCHEMAS.glob("*/*.schema.json"))
# The header hashes tracklab froze (dynamic_events 322 columns, phases_of_play 44).
HEADER_SHA256 = {
    "dynamic_events": "801a67ffefdb68b215b1205318267fae6c3bd14316e1d305ef7775f9b2a74f48",
    "phases_of_play": "1dca435ea3cac484f53916f23db1c99da23ae371dd1d637c1cb95b421a80cab3",
}


def test_the_glob_finds_every_schema_the_repository_froze() -> None:
    """Guards the glob itself: without this, a parametrised run over an empty or
    half-found list would pass while testing nothing. ftbase ships the 2 per-match
    football CSV schemas (the 3 season aggregates stayed in tracklab). A schema added
    or lost changes these numbers on purpose."""
    found = {d.name: len(list(d.glob("*.schema.json"))) for d in sorted(SCHEMAS.iterdir())}
    print(f"\n{len(FROZEN)} frozen schemas under {SCHEMAS}: {found}")
    assert found == {"football": 2}
    assert len(FROZEN) == 2


@pytest.mark.parametrize("path", FROZEN, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_every_frozen_schema_translates_to_pyarrow(path: Path) -> None:
    schema = csv_pa_schema(load_schema(path))
    types = sorted({str(f.type) for f in schema})
    print(f"{path.parent.name}/{path.stem}: {len(schema)} columns, types {types}")
    assert len(schema) > 0


@pytest.mark.parametrize("path", FROZEN, ids=lambda p: p.stem)
def test_header_sha256_is_the_hash_of_the_frozen_column_names(path: Path) -> None:
    """The stored `header_sha256` equals the hash of the comma-joined column names, and
    equals the value tracklab froze: renaming, adding or dropping a column in the JSON
    (or editing the hash) fails here."""
    frozen = load_schema(path)
    recomputed = hashlib.sha256(",".join(frozen.names).encode("utf-8")).hexdigest()
    print(f"{frozen.table}: {len(frozen.names)} columns, stored {frozen.header_sha256}")
    assert recomputed == frozen.header_sha256
    assert frozen.header_sha256 == HEADER_SHA256[frozen.table]


def test_an_unknown_frozen_type_names_the_table_the_column_and_the_type() -> None:
    schema = FrozenSchema(
        table="aggregates_invented",
        header_sha256="00" * 32,
        columns=(Column("player_id", "BIGINT"), Column("signed_at", "TIMESTAMP")),
        all_null_columns=(),
        frozen_from=("data/aggregates/invented.csv",),
    )
    with pytest.raises(ValueError, match=r"aggregates_invented\.signed_at: no pyarrow type"):
        csv_pa_schema(schema)


def test_date_round_trips_through_read_typed_pa_schema_and_parquet(tmp_path: Path) -> None:
    """Building the pyarrow schema is not enough proof on its own: `read_typed`
    (DuckDB) -> `pa_schema` (cast) -> Parquet -> read back must hand back the same
    `player_birthdate` the raw CSV declared."""
    con = duckdb.connect()
    path = tmp_path / "aggregates_physical.csv"
    raw_birthdate = "1993-06-02"
    path.write_text(
        f"player_id,player_birthdate,minutes\n1,{raw_birthdate},812.5\n2,1998-11-20,640.0\n",
        encoding="utf-8",
    )
    frozen = freeze_csv_family("aggregates_physical", [path], tmp_path, con)
    assert {c.name: c.type for c in frozen.columns}["player_birthdate"] == "DATE"

    data = read_typed(frozen, path, con)
    schema = csv_pa_schema(frozen)
    table = pa.Table.from_arrays([data.column(f.name).cast(f.type) for f in schema], schema=schema)

    out_path = tmp_path / "out.parquet"
    pq.write_table(table, out_path)
    written_birthdate = pq.read_table(out_path).column("player_birthdate")[0].as_py()

    print(f"raw csv player_birthdate={raw_birthdate} parquet player_birthdate={written_birthdate}")
    assert written_birthdate == datetime.date(1993, 6, 2)
    assert str(written_birthdate) == raw_birthdate
