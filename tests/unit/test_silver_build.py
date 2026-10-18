# New in ftbase (no tracklab origin): tracklab tested silver through dbt on its fixtures.
"""``ftbase silver`` end to end on a synthetic bronze in ``tmp_path``: no real data.

Two identical matches, 900001 and 900002, home 7 (players 71, and 72 who never plays)
against away 8 (player 81), written as bronze Parquet with only the columns silver reads.
Two, so that a reader asked for one match can be caught reading the other. Every count
below is per match and known by hand:

- periods: 1 is frames 10-14, 2 is frames 20-24; frames 8-9 and 15-19 carry a null
  period, and player 71 is planted at frame 9, so the boundary rule has a row to drop;
- frames: 71 and 81 in each of the 10 period frames (20 rows) plus the ball in 9 of
  them (no ball at frame 12): 29 rows; not detected: 71 at frames 11 and 12, the ball
  at frame 13;
- ``home_team_side`` is ``[right_to_left, left_to_right]``: home attacks towards -x in
  period 1, so its sign is -1 there and +1 in period 2, and away the opposite;
- events: 5 player possessions, 1 off-ball run, 4 phases and 4 team possessions (chain
  rule: e1 and e3 flagged, e4 opens one because the period changed, e5 flagged) = 14.

The assertions are also run against planted violations, one per assertion and one per
kind of schema check, through the same connection a reader gets, so none of them can
pass by construction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from manifest_helper import save_manifest

from ftbase.bronze.common import bronze_path
from ftbase.cli import main
from ftbase.ingest.manifest import Manifest, Source
from ftbase.silver import read_dim, read_events, read_frames
from ftbase.silver.checks import Check, all_checks, assertion_checks, run_check, run_checks
from ftbase.silver.read import connect

MATCH, OTHER, HOME, AWAY = 900001, 900002, 7, 8
COMMIT = "c" * 40
PP, R2L, L2R = "player_possession", "right_to_left", "left_to_right"
PERIOD_FRAMES = [*range(10, 15), *range(20, 25)]

Columns = dict[str, tuple[pa.DataType, list[Any]]]


def _write(data_dir: Path, table: str, columns: Columns, match: int) -> None:
    path = bronze_path(data_dir, "football", table, match)
    path.parent.mkdir(parents=True, exist_ok=True)
    n = len(next(iter(columns.values()))[1])
    columns = {"match_id": (pa.int64(), [match] * n), **columns}
    pq.write_table(pa.table({k: pa.array(v, type=t) for k, (t, v) in columns.items()}), path)


def _period(frame: int) -> int | None:
    return 1 if 10 <= frame <= 14 else 2 if 20 <= frame <= 24 else None


def write_bronze(
    data_dir: Path,
    phase_sides: list[str] | None = None,
    dup_frame: bool = False,
    match: int = MATCH,
) -> None:
    i64, f64, s, b = pa.int64(), pa.float64(), pa.string(), pa.bool_()

    def put(table: str, columns: Columns) -> None:
        _write(data_dir, table, columns, match)

    put(
        "match",
        {
            "id": (i64, [match]),
            "home_team_id": (i64, [HOME]),
            "away_team_id": (i64, [AWAY]),
            "home_team_side": (pa.list_(s), [[R2L, L2R]]),
            "home_team_name": (s, ["Home FC "]),
            "home_team_short_name": (s, ["HOM"]),
            "away_team_name": (s, ["Away FC"]),
            "away_team_short_name": (s, ["AWY"]),
            "home_team_score": (i64, [2]),
            "away_team_score": (i64, [1]),
            "pitch_length": (i64, [105]),
            "pitch_width": (i64, [68]),
            "date_time": (s, ["2024-10-19T09:00:00Z"]),
            "competition_edition_competition_name": (s, ["A-League"]),
            "competition_edition_season_name": (s, ["2024/2025"]),
        },
    )
    put(
        "match_periods",
        {"period": (i64, [1, 2]), "start_frame": (i64, [10, 20]), "end_frame": (i64, [14, 24])},
    )
    put(
        "match_players",
        {
            "id": (i64, [71, 72, 81]),
            "team_id": (i64, [HOME, HOME, AWAY]),
            "first_name": (s, ["Ann", "Bea", "Cid"]),
            "last_name": (s, ["One", "Two", "Three"]),
            "number": (i64, [1, 2, 3]),
        },
    )
    frames = [8, 9, *range(10, 25)]
    put(
        "tracking_frames",
        {
            "frame": (i64, frames),
            "period": (pa.int16(), [_period(f) for f in frames]),
            "ball_x": (f64, [None if _period(f) is None or f == 12 else 0.5 * f for f in frames]),
            "ball_y": (f64, [None if _period(f) is None or f == 12 else 1.0 for f in frames]),
            "ball_z": (f64, [None if _period(f) is None or f == 12 else 0.2 for f in frames]),
            "ball_is_detected": (b, [None if _period(f) is None else f != 13 for f in frames]),
        },
    )
    objects = [(9, 71), *((f, p) for f in PERIOD_FRAMES for p in (71, 81))]
    if dup_frame:
        objects.append((10, 71))
    put(
        "tracking_objects",
        {
            "frame": (i64, [f for f, _ in objects]),
            "player_id": (i64, [p for _, p in objects]),
            "x": (f64, [float(f) if p == 71 else -float(f) for f, p in objects]),
            "y": (f64, [2.0 if p == 71 else -2.0 for _, p in objects]),
            "is_detected": (b, [not (p == 71 and f in (11, 12)) for f, p in objects]),
        },
    )
    put(
        "dynamic_events",
        {
            "event_id": (s, ["e1", "e2", "e3", "e4", "e5", "r1"]),
            "event_type": (s, [PP, PP, PP, PP, PP, "off_ball_run"]),
            "event_subtype": (s, [None, None, None, None, None, "run_ahead"]),
            "period": (i64, [1, 1, 1, 2, 2, 1]),
            "frame_start": (i64, [10, 12, 13, 20, 22, 13]),
            "frame_end": (i64, [11, 12, 14, 21, 24, 14]),
            "team_id": (i64, [HOME, HOME, AWAY, AWAY, HOME, AWAY]),
            "player_id": (i64, [71, 71, 81, 81, 71, 81]),
            "player_in_possession_id": (i64, [None, None, None, None, None, 81]),
            "x_start": (f64, [5.0, 6.0, 7.0, 8.0, 9.0, 3.0]),
            "y_start": (f64, [2.0, 2.0, 2.0, 2.0, 2.0, 1.0]),
            "x_end": (f64, [6.0, 7.0, 8.0, 9.0, 10.0, 4.0]),
            "y_end": (f64, [3.0, 3.0, 3.0, 3.0, 3.0, 1.0]),
            "attacking_side": (s, [R2L, R2L, L2R, R2L, L2R, L2R]),
            "end_type": (s, ["pass", "possession_loss", "shot", "possession_loss", "pass", None]),
            "first_player_possession_in_team_possession": (
                b,
                [True, False, True, False, True, None],
            ),
        },
    )
    put(
        "phases_of_play",
        {
            "index": (i64, [0, 1, 2, 3]),
            "period": (i64, [1, 1, 2, 2]),
            "frame_start": (i64, [10, 13, 20, 23]),
            "frame_end": (i64, [12, 14, 22, 24]),
            "team_in_possession_id": (i64, [HOME, AWAY, HOME, AWAY]),
            "x_start": (f64, [1.0, 2.0, 3.0, 4.0]),
            "y_start": (f64, [0.0, 0.0, 0.0, 0.0]),
            "x_end": (f64, [2.0, 3.0, 4.0, 5.0]),
            "y_end": (f64, [1.0, 1.0, 1.0, 1.0]),
            "attacking_side": (s, phase_sides or [R2L, L2R, L2R, R2L]),
            "team_in_possession_phase_type": (s, ["build_up", "transition", "create", "chaotic"]),
            "team_possession_lead_to_goal": (b, [False, True, False, False]),
            "team_possession_lead_to_shot": (b, [False, True, False, False]),
            "team_possession_loss_in_phase": (b, [True, False, False, True]),
        },
    )


def write_manifest(path: Path) -> None:
    source = Source(
        source_id="opendata-football",
        sport="football",
        kind="github",
        repo="SkillCorner/opendata",
        ref="master",
        commit=COMMIT,
        license="MIT",
        raw_url="https://raw.example/{commit}/",
        lfs_url="https://lfs.example/{commit}/",
        files=[],
    )
    save_manifest(Manifest(schema_version=1, sources=[source]), path)


def run_silver(tmp_path: Path) -> int:
    write_manifest(tmp_path / "manifest.json")
    return main(
        ["silver", "--data-dir", str(tmp_path), "--manifest", str(tmp_path / "manifest.json")]
    )


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Path:
    data_dir = tmp_path_factory.mktemp("silver")
    write_bronze(data_dir)
    write_bronze(data_dir, match=OTHER)
    assert run_silver(data_dir) == 0
    return data_dir


# --- the build ----------------------------------------------------------------------------


def test_silver_writes_every_table_with_the_planted_counts(built: Path) -> None:
    silver = built / "silver"
    expected = {
        "dim_sport": 1,
        "dim_period": 4,
        "dim_team": 2,
        "dim_match": 2,
        "dim_team_period": 8,
        "dim_entity": 8,  # per match, three players on the roster and the ball
    }
    for name, n in expected.items():
        assert pq.read_table(silver / name / "data.parquet").num_rows == n, name
    for match in (MATCH, OTHER):
        assert pq.read_table(silver / "frames" / f"{match}.parquet").num_rows == 29
        assert pq.read_table(silver / "events" / f"{match}.parquet").num_rows == 14
    assert not list(silver.rglob("*.tmp")), "temporary files are renamed, never left behind"


def test_every_check_passes_and_there_is_one_per_assertion_file(built: Path) -> None:
    with connect(built) as con:
        results = run_checks(con)
    assert [r.name for r in results if not r.ok] == []
    assert len(assertion_checks()) == 6
    assert len(results) == len(all_checks())


def test_dims_carry_keys_names_and_the_pinned_commit(built: Path) -> None:
    matches = {r["match_id"]: r for r in read_dim("dim_match", data_dir=built).to_pylist()}
    assert set(matches) == {f"fb:{MATCH}", f"fb:{OTHER}"}
    match = matches[f"fb:{MATCH}"]
    assert (match["home_team_id"], match["away_team_id"]) == (f"fb:{HOME}", f"fb:{AWAY}")
    assert match["surface_spec"] == "provider_105x68"
    assert match["n_periods"] == 2
    assert match["source_commit"] == COMMIT
    teams = {r["team_id"]: r["name"] for r in read_dim("dim_team", data_dir=built).to_pylist()}
    assert teams == {f"fb:{HOME}": "Home FC", f"fb:{AWAY}": "Away FC"}, "names are trimmed"
    signs = {
        (r["period"], r["team_id"]): r["attack_sign"]
        for r in read_dim("dim_team_period", data_dir=built).to_pylist()
        if r["match_id"] == f"fb:{MATCH}"
    }
    assert signs == {(1, "fb:7"): -1, (1, "fb:8"): 1, (2, "fb:7"): 1, (2, "fb:8"): -1}


def test_frames_follow_the_boundary_rule_and_keep_is_detected(built: Path) -> None:
    rows = read_frames([MATCH], data_dir=built).to_pylist()
    assert {r["frame_native"] for r in rows} == set(PERIOD_FRAMES), "null-period frames dropped"
    undetected = {(r["frame_native"], r["entity_id"]) for r in rows if not r["is_detected"]}
    assert undetected == {(11, "fb:71"), (12, "fb:71"), (13, "fb:ball")}
    ball = [r for r in rows if r["entity_id"] == "fb:ball"]
    assert len(ball) == 9 and all(r["z_m"] == 0.2 and r["team_id"] is None for r in ball)
    assert all(r["z_m"] is None for r in rows if r["entity_kind"] == "player")
    r = next(r for r in rows if r["frame_native"] == 22 and r["entity_id"] == "fb:81")
    assert (r["t_ms"], r["clock_s"], r["x_m"], r["team_id"]) == (2200, 0.2, -22.0, "fb:8")


def test_read_frames_contract(built: Path) -> None:
    table = read_frames([MATCH], data_dir=built)
    assert table.schema.names == [
        "sport",
        "match_id",
        "native_match_id",
        "period",
        "frame_native",
        "t_ms",
        "clock_s",
        "entity_id",
        "entity_kind",
        "team_id",
        "x_m",
        "y_m",
        "z_m",
        "is_detected",
    ]
    types = dict(zip(table.schema.names, table.schema.types, strict=True))
    assert types["period"] == pa.int16() and types["frame_native"] == pa.int64()
    assert types["is_detected"] == pa.bool_() and types["x_m"] == pa.float64()
    keys = [(r["frame_native"], r["entity_id"]) for r in table.to_pylist()]
    assert keys == sorted(keys), "ordered by match, frame, entity"
    picked = read_frames([MATCH], columns=["entity_id", "is_detected"], data_dir=built)
    assert picked.schema.names == ["entity_id", "is_detected"] and picked.num_rows == 29


def test_read_frames_attacking_turns_the_team_in_possession_towards_plus_x(built: Path) -> None:
    rows = read_frames([MATCH], attacking=True, data_dir=built).to_pylist()
    assert len(rows) == 29, "every period frame lies inside a team possession here"
    by_key = {(r["frame_native"], r["entity_id"]): r for r in rows}
    home_p1 = by_key[(10, "fb:71")]  # home in possession, period 1: home attacks towards -x
    assert (home_p1["team_in_possession_id"], home_p1["attack_sign"]) == ("fb:7", -1)
    assert (home_p1["x_m"], home_p1["x_att_m"], home_p1["y_att_m"]) == (10.0, -10.0, -2.0)
    away_p2 = by_key[(20, "fb:81")]  # away in possession, period 2: away attacks towards -x
    assert (away_p2["team_in_possession_id"], away_p2["attack_sign"]) == ("fb:8", -1)
    assert away_p2["x_att_m"] == 20.0


def test_read_events_rotate_to_the_venue_frame_and_derive_possessions(built: Path) -> None:
    rows = read_events([MATCH], data_dir=built).to_pylist()
    kinds = sorted((r["event_type"], r["native_family"]) for r in rows)
    assert kinds.count(("possession", "possession")) == 4
    assert kinds.count(("pass", PP)) == 2 and kinds.count((PP, PP)) == 3
    assert kinds.count(("other", "phases_of_play")) == 4
    assert kinds.count(("other", "off_ball_run")) == 1
    by_id = {r["event_id"]: r for r in rows}
    e1 = by_id[f"fb:{PP}:{MATCH}:e1"]
    assert (e1["x_start_m"], e1["y_start_m"]) == (-5.0, -2.0), "right_to_left turns (x, y)"
    assert by_id[f"fb:{PP}:{MATCH}:e5"]["x_start_m"] == 9.0, "left_to_right keeps (x, y)"
    possessions = sorted(
        (r["frame_start"], r["frame_end"], r["team_id"])
        for r in rows
        if r["event_type"] == "possession"
    )
    assert possessions == [(10, 12, "fb:7"), (13, 14, "fb:8"), (20, 21, "fb:8"), (22, 24, "fb:7")]
    assert e1["possession_id"] == f"fb:possession:{MATCH}:1"


def test_readers_open_only_the_matches_they_are_given(built: Path) -> None:
    for match in (MATCH, OTHER):
        frames = read_frames([match], columns=["native_match_id"], data_dir=built)
        assert set(frames.column("native_match_id").to_pylist()) == {match}
        assert frames.num_rows == 29
        attacking = read_frames([match], attacking=True, data_dir=built)
        assert set(attacking.column("native_match_id").to_pylist()) == {match}
        events = read_events([match], columns=["match_id"], data_dir=built)
        assert set(events.column("match_id").to_pylist()) == {f"fb:{match}"}
    both = read_frames([OTHER, MATCH], columns=["native_match_id"], data_dir=built)
    assert both.column("native_match_id").to_pylist() == [MATCH] * 29 + [OTHER] * 29


def test_readers_refuse_what_they_cannot_read(built: Path) -> None:
    with pytest.raises(ValueError, match="at least one match"):
        read_frames([], data_dir=built)
    with pytest.raises(FileNotFoundError, match="silver files missing"):
        read_frames([900009], data_dir=built)
    with pytest.raises(ValueError, match=r"no column\(s\) \['nope'\]"):
        read_events([MATCH], columns=["nope"], data_dir=built)


# --- the build fails loudly ----------------------------------------------------------------


def test_a_phase_against_home_team_side_fails_the_build(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_bronze(tmp_path, phase_sides=[L2R, L2R, L2R, R2L])
    assert run_silver(tmp_path) == 1
    assert f"FAIL orientation {MATCH}: phases agree 3/4" in capsys.readouterr().out


def test_a_duplicated_tracking_row_fails_the_build(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_bronze(tmp_path, dup_frame=True)
    assert run_silver(tmp_path) == 1
    out = capsys.readouterr().out
    assert "FAIL unique frames (match_id, frame_native, entity_id): 1 row(s)" in out
    assert "FAIL assert_frames_t_ms_increases: 1 row(s)" in out


def test_no_bronze_is_a_clear_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match=r"no bronze match under .*run `ftbase bronze` first"):
        run_silver(tmp_path)


# --- every assertion and every kind of check can fail ---------------------------------------

PLANTED: dict[str, tuple[str, str]] = {
    # assertion or check name -> (relation to replace, its planted replacement)
    "assert_attack_signs_opposite": (
        "dim_team_period",
        "SELECT * REPLACE (abs(attack_sign) AS attack_sign) FROM read_parquet({dim_team_period})",
    ),
    "assert_football_possessions_do_not_overlap": (
        "events",
        "SELECT * REPLACE (CASE WHEN event_type = 'possession' AND frame_start = 13 "
        "THEN 12 ELSE frame_start END AS frame_start) FROM read_parquet({events})",
    ),
    "assert_frames_entities_in_dim_entity": (
        "dim_entity",
        "SELECT * FROM read_parquet({dim_entity}) WHERE entity_id <> 'fb:81'",
    ),
    "assert_frames_t_ms_increases": (
        "frames",
        "SELECT * FROM read_parquet({frames}) UNION ALL "
        "(SELECT * FROM read_parquet({frames}) LIMIT 1)",
    ),
    "assert_frames_z_only_on_the_ball": (
        "frames",
        "SELECT * REPLACE (coalesce(z_m, 0.0) AS z_m) FROM read_parquet({frames})",
    ),
    "assert_one_ball_per_match": (
        "dim_entity",
        "SELECT * FROM read_parquet({dim_entity}) UNION ALL "
        "SELECT * FROM read_parquet({dim_entity}) WHERE entity_kind = 'ball'",
    ),
    "unique dim_team (team_id)": (
        "dim_team",
        "SELECT * FROM read_parquet({dim_team}) UNION ALL "
        "(SELECT * FROM read_parquet({dim_team}) LIMIT 1)",
    ),
    "not_null dim_match.home_team_id": (
        "dim_match",
        "SELECT * REPLACE (NULL::VARCHAR AS home_team_id) FROM read_parquet({dim_match})",
    ),
    "relationships frames.team_id -> dim_team.team_id": (
        "frames",
        "SELECT * REPLACE (CASE WHEN team_id = 'fb:8' THEN 'fb:999' ELSE team_id END AS team_id) "
        "FROM read_parquet({frames})",
    ),
}


@pytest.mark.parametrize("name", sorted(PLANTED))
def test_each_check_fails_on_its_planted_violation(built: Path, name: str) -> None:
    checks: dict[str, Check] = {c.name: c for c in all_checks()}
    relation, planted = PLANTED[name]
    files = {
        "frames": built / "silver" / "frames" / f"{MATCH}.parquet",
        "events": built / "silver" / "events" / f"{MATCH}.parquet",
        **{
            d: built / "silver" / d / "data.parquet"
            for d in ("dim_team_period", "dim_entity", "dim_team", "dim_match")
        },
    }
    sql = planted.format(**{k: f"'{v.as_posix()}'" for k, v in files.items()})
    with connect(built) as con:
        assert run_check(con, checks[name]).ok, "passes before the violation is planted"
        con.execute(f"CREATE OR REPLACE VIEW {relation} AS {sql}")
        result = run_check(con, checks[name])
    assert result.n_rows > 0 and result.sample, f"{name} did not see the planted violation"
