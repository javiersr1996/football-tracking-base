"""bronze football events (`dynamic_events`, `phases_of_play`) on synthetic CSVs.

No real data, no 322/44-column fixture: the frozen schemas come from
`formats.csvschema.freeze_csv_family` over small synthetic CSVs (never hand-written
JSON), the same tactic `tests/unit/test_bronze_basketball_events.py` uses via
`freeze_family`. `build_events` itself is exercised end to end through the CLI by
monkeypatching `load_frozen_tables` in the module namespace (read fresh at call
time, so the patch reaches it), the same trick that test uses for `FAMILY_ORDER`.

Covers: the native `match_id` column folded into the lineage one (`table_schema`),
that fold's row-by-row check failing loudly on a mismatch, a header that drifted
from the frozen one failing through `read_typed`, and nulls carried through as-is
(no orphan is ever dropped, matching docs/formats/football.md).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest
from manifest_helper import save_manifest

import ftbase.bronze.football.events as events_module
from ftbase.bronze.common import bronze_path
from ftbase.bronze.football.events import build_table_match, table_schema
from ftbase.cli import main
from ftbase.formats.csvschema import freeze_csv_family
from ftbase.ingest.fetch import RECEIPT_NAME
from ftbase.ingest.manifest import FileEntry, Manifest, Source

SPORT = "football"
SOURCE_ID = "opendata-football"
COMMIT = "c0ffee" * 6 + "cafe"
MATCH_A, MATCH_B = 900001, 900002

EVENTS_HEADER = "match_id,event_id,event_type,player_id,x_start"
PHASES_HEADER = "match_id,index,team_in_possession_id,duration"


def events_path(match_id: int) -> str:
    return f"data/matches/{match_id}/{match_id}_dynamic_events.csv"


def phases_path(match_id: int) -> str:
    return f"data/matches/{match_id}/{match_id}_phases_of_play.csv"


def cell(value: object) -> str:
    return "" if value is None else str(value)


def events_csv(match_id: int, rows: list[tuple[int, str, int | None, float | None]]) -> str:
    lines = [EVENTS_HEADER]
    lines += [f"{match_id},{cell(e)},{t},{cell(p)},{cell(x)}" for e, t, p, x in rows]
    return "\n".join(lines) + "\n"


def phases_csv(match_id: int, rows: list[tuple[int, int, float]]) -> str:
    lines = [PHASES_HEADER]
    lines += [f"{match_id},{idx},{team},{dur}" for idx, team, dur in rows]
    return "\n".join(lines) + "\n"


EVENTS_A = [(1, "pass", 10, 1.5), (2, "shot", None, None), (3, "run", 11, -3.25)]
EVENTS_B = [(1, "pass", 12, 0.0)]
PHASES_A = [(0, 500, 4.5), (1, 501, 2.0)]
PHASES_B = [(0, 500, 6.0)]


def write(path: Path, content: str) -> bytes:
    blob = content.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)
    return blob


def entry_for(path: str, match_id: int, role: str, blob: bytes) -> FileEntry:
    return FileEntry(
        path=path,
        bytes=len(blob),
        sha256=hashlib.sha256(blob).hexdigest(),
        lfs=True,
        role=role,
        match_id=match_id,
    )


def rows(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


# --- table_schema: the native `match_id` column is folded into lineage ---------------


@pytest.fixture
def dynamic_events_frozen(tmp_path: Path) -> tuple[Any, Any, Path]:
    con = duckdb.connect()
    clone = tmp_path / "clone"
    path_a = clone / events_path(MATCH_A)
    path_b = clone / events_path(MATCH_B)
    write(path_a, events_csv(MATCH_A, EVENTS_A))
    write(path_b, events_csv(MATCH_B, EVENTS_B))
    frozen = freeze_csv_family("dynamic_events", [path_a, path_b], clone, con)
    return frozen, con, clone


def test_table_schema_folds_native_match_id(dynamic_events_frozen: tuple[Any, Any, Path]) -> None:
    frozen, _con, _clone = dynamic_events_frozen
    schema = table_schema(frozen)
    assert schema.names == [
        "sport",
        "match_id",
        "source_file",
        "source_row",
        "event_id",
        "event_type",
        "player_id",
        "x_start",
    ], "the native match_id column must not appear a second time"


def test_build_table_match_writes_lineage_and_native_columns(
    dynamic_events_frozen: tuple[Any, Any, Path], tmp_path: Path
) -> None:
    frozen, con, clone = dynamic_events_frozen
    schema = table_schema(frozen)
    data_dir = tmp_path / "data"
    blob = write(clone / events_path(MATCH_A), events_csv(MATCH_A, EVENTS_A))
    entry = entry_for(events_path(MATCH_A), MATCH_A, "events", blob)

    s = build_table_match("dynamic_events", frozen, schema, entry, clone, COMMIT, data_dir, con)
    assert (s.match_id, s.table, s.rows) == (MATCH_A, "dynamic_events", 3)

    out = rows(bronze_path(data_dir, SPORT, "dynamic_events", MATCH_A))
    assert [r["event_id"] for r in out] == [1, 2, 3]
    assert [r["source_row"] for r in out] == [0, 1, 2]
    assert [r["match_id"] for r in out] == [MATCH_A] * 3, "folded from native match_id"
    assert [r["player_id"] for r in out] == [10, None, 11], "the null orphan-shaped value is kept"
    assert [r["x_start"] for r in out] == [1.5, None, -3.25]
    assert all(r["sport"] == SPORT and r["source_file"] == events_path(MATCH_A) for r in out)

    written = pq.read_schema(bronze_path(data_dir, SPORT, "dynamic_events", MATCH_A))
    assert written.equals(schema, check_metadata=False)


def test_native_match_id_mismatch_fails_loudly(
    dynamic_events_frozen: tuple[Any, Any, Path], tmp_path: Path
) -> None:
    """A row whose native `match_id` disagrees with the file's own match id (never
    observed in the real bundle, 0/20 files) must fail naming the file and the row,
    not be silently trusted."""
    frozen, con, clone = dynamic_events_frozen
    schema = table_schema(frozen)
    data_dir = tmp_path / "data"
    # entry.match_id claims MATCH_B, but the CSV content is MATCH_A's.
    blob = (clone / events_path(MATCH_A)).read_bytes()
    entry = entry_for(events_path(MATCH_A), MATCH_B, "events", blob)

    with pytest.raises(ValueError, match=rf"row 0: match_id {MATCH_A} != {MATCH_B}"):
        build_table_match("dynamic_events", frozen, schema, entry, clone, COMMIT, data_dir, con)
    assert not bronze_path(data_dir, SPORT, "dynamic_events", MATCH_B).exists()


def test_drifted_header_fails_through_read_typed(
    dynamic_events_frozen: tuple[Any, Any, Path], tmp_path: Path
) -> None:
    frozen, con, clone = dynamic_events_frozen
    schema = table_schema(frozen)
    data_dir = tmp_path / "data"
    bad_path = clone / "data/matches/900003/900003_dynamic_events.csv"
    write(bad_path, "match_id,event_id,event_type,player_id\n900003,1,pass,1\n")  # missing x_start
    entry = entry_for(
        "data/matches/900003/900003_dynamic_events.csv", 900003, "events", bad_path.read_bytes()
    )

    with pytest.raises(ValueError, match="header does not match frozen dynamic_events"):
        build_table_match("dynamic_events", frozen, schema, entry, clone, COMMIT, data_dir, con)


# --- build_events: both tables, both matches, through the CLI ------------------------


def frozen_source(files: list[FileEntry]) -> Source:
    repo = "SkillCorner/opendata"
    return Source(
        source_id=SOURCE_ID,
        sport=SPORT,
        kind="github",
        repo=repo,
        ref="master",
        commit=COMMIT,
        license="MIT",
        raw_url=f"https://raw.githubusercontent.com/{repo}/{{commit}}/",
        lfs_url=f"https://media.githubusercontent.com/media/{repo}/{{commit}}/",
        files=files,
    )


def write_receipt(data_dir: Path, source_path: Path) -> None:
    receipt_dir = data_dir / "raw" / SOURCE_ID
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt = {
        "source_id": SOURCE_ID,
        "commit": COMMIT,
        "verified": 4,
        "downloaded": 0,
        "failed": [],
        "finished_at": "2026-09-22T10:00:00+00:00",
        "source_path": str(source_path),
    }
    (receipt_dir / RECEIPT_NAME).write_text(json.dumps(receipt), encoding="utf-8")


@pytest.fixture
def bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    clone = tmp_path / "clone"
    data_dir = tmp_path / "data"
    entries = []
    per_match = ((MATCH_A, EVENTS_A, PHASES_A), (MATCH_B, EVENTS_B, PHASES_B))
    for match_id, ev_rows, ph_rows in per_match:
        ev_blob = write(clone / events_path(match_id), events_csv(match_id, ev_rows))
        ph_blob = write(clone / phases_path(match_id), phases_csv(match_id, ph_rows))
        entries.append(entry_for(events_path(match_id), match_id, "events", ev_blob))
        entries.append(entry_for(phases_path(match_id), match_id, "phases", ph_blob))
    manifest_path = tmp_path / "sources.json"
    save_manifest(Manifest(schema_version=1, sources=[frozen_source(entries)]), manifest_path)
    write_receipt(data_dir, clone)

    con = duckdb.connect()
    frozen = {
        "dynamic_events": freeze_csv_family(
            "dynamic_events",
            [clone / events_path(MATCH_A), clone / events_path(MATCH_B)],
            clone,
            con,
        ),
        "phases_of_play": freeze_csv_family(
            "phases_of_play",
            [clone / phases_path(MATCH_A), clone / phases_path(MATCH_B)],
            clone,
            con,
        ),
    }
    # load_frozen_tables is looked up fresh from the module namespace at call time
    # inside build_events, so patching it here reaches the CLI path without needing
    # a real 322/44-column schemas directory (mirrors FAMILY_ORDER in the basketball
    # events test).
    monkeypatch.setattr(events_module, "load_frozen_tables", lambda: frozen)
    return manifest_path, data_dir


def run_events(manifest_path: Path, data_dir: Path) -> int:
    return main(
        [
            "bronze",
            "--tables",
            "events",
            "--manifest",
            str(manifest_path),
            "--data-dir",
            str(data_dir),
        ]
    )


def test_cli_builds_both_tables_for_both_matches(
    bundle: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path, data_dir = bundle
    rc = run_events(manifest_path, data_dir)
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("bronze events: ")
    assert out[1].split() == ["match_id", "table", "rows", "seconds", "MB"]
    body = [line.split()[:3] for line in out[2:-1]]
    assert body == [
        [str(MATCH_A), "dynamic_events", "3"],
        [str(MATCH_A), "phases_of_play", "2"],
        [str(MATCH_B), "dynamic_events", "1"],
        [str(MATCH_B), "phases_of_play", "1"],
    ], "match_id order, dynamic_events before phases_of_play"
    assert out[-1].startswith("total seconds=")

    for table, match_id, n in (
        ("dynamic_events", MATCH_A, 3),
        ("phases_of_play", MATCH_A, 2),
        ("dynamic_events", MATCH_B, 1),
        ("phases_of_play", MATCH_B, 1),
    ):
        path = bronze_path(data_dir, SPORT, table, match_id)
        assert path.is_file()
        assert pq.read_table(path).num_rows == n
