# New in ftbase (no tracklab origin): tracklab's silver contracts ran through dbt and its
# tests/contracts/test_{frames,events,dims}.py, which mixed both sports and are not copied.
"""Contract of the football silver against the real bundle.

Marked `realdata` (deselected by default, never skipped): run them with
`FTBASE_DATA_ROOT=<dir> uv run pytest -m realdata` after `ftbase ingest`, `ftbase bronze`
and `ftbase silver`.

The pinned counts are the ones compared row by row against tracklab's silver
(tracklab@686508a, read-only): the same rows per match, the same ``is_detected`` split
and ``EXCEPT ALL`` empty both ways in the 20 matches.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from ftbase.paths import data_root
from ftbase.silver import read_dim, read_frames
from ftbase.silver.checks import run_checks
from ftbase.silver.read import built_match_ids, connect

DATA = data_root()
EXPECTED_FRAMES = 20_213_849  # 19,334,986 player rows + 878,863 ball rows
EXPECTED_DETECTED = 11_683_552
EXPECTED_EVENTS = 107_204
EXPECTED_FRAMES_ATTACKING = 15_284_167

pytestmark = pytest.mark.realdata


def test_twenty_matches_were_built() -> None:
    assert len(built_match_ids(DATA)) == 20


def test_every_check_passes_on_the_real_silver() -> None:
    with connect(DATA) as con:
        results = run_checks(con)
    for r in results:
        print(f"{r.name}: {r.n_rows}")
    assert [(r.name, r.n_rows) for r in results if not r.ok] == []


def test_pinned_counts() -> None:
    with connect(DATA) as con:
        row = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE is_detected), "
            "count(*) FILTER (WHERE entity_kind = 'ball') FROM frames"
        ).fetchone()
        assert row is not None
        frames, detected, ball = row
        events = con.execute("SELECT count(*) FROM events").fetchone()
        attacking = con.execute("SELECT count(*) FROM frames_attacking").fetchone()
    assert (frames, detected, ball) == (EXPECTED_FRAMES, EXPECTED_DETECTED, 878_863)
    assert events == (EXPECTED_EVENTS,)
    assert attacking == (EXPECTED_FRAMES_ATTACKING,)


def test_two_teams_with_opposite_signs_in_every_period() -> None:
    rows = read_dim("dim_team_period", data_dir=DATA).to_pylist()
    assert len(rows) == 80
    by_period: dict[tuple[str, int], list[int]] = {}
    for r in rows:
        by_period.setdefault((r["match_id"], r["period"]), []).append(r["attack_sign"])
        assert r["evidence"].endswith(" phases")
    assert len(by_period) == 40
    assert all(sorted(signs) == [-1, 1] for signs in by_period.values())


def test_read_frames_returns_one_match_with_the_documented_types() -> None:
    match_id = built_match_ids(DATA)[0]
    table = read_frames([match_id], data_dir=DATA)
    assert set(table.column("native_match_id").to_pylist()) == {match_id}
    types = dict(zip(table.schema.names, table.schema.types, strict=True))
    assert types["is_detected"] == pa.bool_() and types["period"] == pa.int16()
    assert table.column("is_detected").null_count == 0
