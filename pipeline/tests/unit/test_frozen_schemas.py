"""Every frozen schema on disk must translate to pyarrow, whatever types it carries.

`formats.csvschema.pa_schema` mapped four DuckDB types until task 8 built football's
season aggregates and hit `player_birthdate`, a DATE. The type had been frozen by task
4 long before; nothing was wrong with it. The gap was that the only schemas that had
ever gone through `pa_schema` were basketball's 3 aggregates, which are BOOLEAN,
BIGINT, DOUBLE and VARCHAR only, so no test could have caught it.

This module closes that by iterating the schema files that exist rather than a list
written by hand: a type frozen tomorrow fails here, with the file that carries it,
instead of half-way through a build.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tracklab.formats.basketball import load_family
from tracklab.formats.basketball import pa_schema as family_pa_schema
from tracklab.formats.csvschema import (
    Column,
    FrozenSchema,
    freeze_csv_family,
    load_schema,
    read_typed,
)
from tracklab.formats.csvschema import pa_schema as csv_pa_schema

SCHEMAS = Path(__file__).resolve().parents[1].parent / "src" / "tracklab" / "bronze" / "schemas"
FROZEN = sorted(SCHEMAS.glob("*/*.schema.json"))


def test_the_glob_finds_every_schema_the_repository_froze() -> None:
    """Guards the glob itself: without this, a parametrised run over an empty or
    half-found list would pass while testing nothing. Basketball froze 20 event
    families plus 3 season aggregates (task 6), football 2 per-match CSVs plus 3
    season aggregates (task 4). A schema added or lost changes these numbers on
    purpose, so the test is meant to be updated by the task that moves them."""
    found = {d.name: len(list(d.glob("*.schema.json"))) for d in sorted(SCHEMAS.iterdir())}
    print(f"\n{len(FROZEN)} frozen schemas under {SCHEMAS}: {found}")
    assert found == {"basketball": 23, "football": 5}
    assert len(FROZEN) == 28


@pytest.mark.parametrize("path", FROZEN, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_every_frozen_schema_translates_to_pyarrow(path: Path) -> None:
    """A JSON event family goes through `formats.basketball.pa_schema` (it knows
    POINT2D and the lists), a CSV family through `formats.csvschema.pa_schema`. Both
    mappings are exercised by whichever files are on disk."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if "family" in raw:
        schema = family_pa_schema(load_family(path))
    else:
        schema = csv_pa_schema(load_schema(path))
    types = sorted({str(f.type) for f in schema})
    print(f"{path.parent.name}/{path.stem}: {len(schema)} columns, types {types}")
    assert len(schema) > 0


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
