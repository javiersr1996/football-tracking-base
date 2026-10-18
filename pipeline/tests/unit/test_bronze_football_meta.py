"""bronze football meta (match, match_periods, match_players, matches) on a synthetic
bundle. No real data, no network.

Two matches exercise the two ``players[]`` regimes measured while writing
``bronze/football/meta.py`` (see its module docstring): match A carries
``last_modified`` and ``playing_time.sequences`` on every player, match B has neither
key at all. Each match also has one player who never took the pitch
(``start_time``/``end_time`` null, ``playing_time.total`` null, ``by_period`` empty),
to check that bronze keeps ``playing_time_total_*`` all-NULL rather than inventing
zeros. Run through the CLI exactly as a user would (``tracklab bronze football --tables
meta``), then every table is read back from Parquet and checked against the planted
values. The key-set violation tests call the flattening helpers directly, the same way
the basketball tests reach ``frame_batches`` directly, since the whole point of this
task's schema discipline is that a bad key set fails before a single column is built.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from tracklab.bronze.common import bronze_path, bronze_season_path
from tracklab.bronze.football.meta import (
    _check_keys,
    _match_row,
    _period_columns,
    _player_columns,
    build_match_tables,
)
from tracklab.cli import main
from tracklab.ingest.fetch import RECEIPT_NAME
from tracklab.ingest.manifest import FileEntry, Manifest, Source, save_manifest

SOURCE_ID = "opendata-football"
COMMIT = "4340d274572876239c154c90bc507a9b3250a656"
MATCH_A, MATCH_B = 8100001, 8100002
HOME_ID, AWAY_ID = 1802, 871


def match_json_path(match_id: int) -> str:
    return f"data/matches/{match_id}/{match_id}_match.json"


def _kit(kit_id: int, team_id: int, jersey: str, number: str) -> dict[str, Any]:
    return {
        "id": kit_id,
        "team_id": team_id,
        "name": "Home",
        "jersey_color": jersey,
        "number_color": number,
        "season": {"id": 7, "start_year": 2020, "end_year": 2021, "name": "2020/2021"},
    }


def _competition_edition() -> dict[str, Any]:
    return {
        "id": 870,
        "name": "AUS - A-League - 2024/2025",
        "competition": {
            "id": 61,
            "area": "AUS",
            "name": "A-League",
            "gender": "male",
            "age_group": "adult",
        },
        "season": {"id": 95, "start_year": 2024, "end_year": 2025, "name": "2024/2025"},
    }


def _player(
    player_id: int, team_id: int, number: int, with_optional: bool, played: bool
) -> dict[str, Any]:
    total = (
        {
            "minutes_tip": 10.28,
            "minutes_otip": 8.12,
            "start_frame": 10,
            "end_frame": 19869,
            "minutes_played": 33.1,
            "minutes_played_regular_time": 33.1,
        }
        if played
        else None
    )
    by_period = (
        [
            {
                "name": "period_1",
                "minutes_tip": 10.28,
                "minutes_otip": 8.12,
                "start_frame": 10,
                "end_frame": 19869,
                "minutes_played": 33.1,
            }
        ]
        if played
        else []
    )
    row: dict[str, Any] = {
        "player_role": {
            "id": 3,
            "position_group": "Central Defender",
            "name": "Left Center Back",
            "acronym": "LCB",
        },
        "start_time": "00:00:00" if played else None,
        "end_time": "00:33:06" if played else None,
        "number": number,
        "yellow_card": 0,
        "red_card": 0,
        "injured": False,
        "goal": 0,
        "own_goal": 0,
        "playing_time": {"total": total, "by_period": by_period},
        "team_player_id": player_id + 1_000_000,
        "team_id": team_id,
        "id": player_id,
        "first_name": "First",
        "last_name": "Last",
        "short_name": "F. Last",
        "birthday": "1996-09-04",
        "trackable_object": player_id + 2_000_000,
        "gender": "male",
    }
    if with_optional:
        row["last_modified"] = "2026-04-01T13:19:03.072753Z"
        row["playing_time"]["sequences"] = (
            [{"period": "period_1", "start_frame": 10, "end_frame": 19869}] if played else []
        )
    return row


def match_json(match_id: int, with_optional_regime: bool) -> dict[str, Any]:
    return {
        "id": match_id,
        "home_team_score": 1,
        "away_team_score": 0,
        "date_time": "2024-12-21T06:00:00Z",
        "stadium": {"id": 2914, "name": "Kayo Stadium", "city": "Moreton Bay", "capacity": 11507},
        "home_team": {
            "id": HOME_ID,
            "name": "Brisbane Roar FC",
            "short_name": "Brisbane FC",
            "acronym": "BRR",
        },
        "home_team_kit": _kit(4273, HOME_ID, "#ff8000", "#ffffff"),
        "away_team": {
            "id": AWAY_ID,
            "name": "Perth Glory FC",
            "short_name": "Perth Glory",
            "acronym": "PER",
        },
        "away_team_kit": _kit(4272, AWAY_ID, "#ffffff", "#000000"),
        "home_team_coach": None,
        "away_team_coach": None,
        "home_team_playing_time": {"minutes_tip": 37.19, "minutes_otip": 22.84},
        "away_team_playing_time": {"minutes_tip": 22.84, "minutes_otip": 37.19},
        "competition_edition": _competition_edition(),
        "match_periods": [
            {
                "period": 1,
                "name": "period_1",
                "start_frame": 10,
                "end_frame": 30240,
                "duration_frames": 30230,
                "duration_minutes": 50.38,
            },
            {
                "period": 2,
                "name": "period_2",
                "start_frame": 30250,
                "end_frame": 61300,
                "duration_frames": 31050,
                "duration_minutes": 51.75,
            },
        ],
        "competition_round": {
            "id": 614,
            "name": "Round 9",
            "round_number": 9,
            "potential_overtime": False,
        },
        "referees": [],
        "players": [
            _player(HOME_ID * 100 + 1, HOME_ID, 4, with_optional_regime, played=True),
            _player(AWAY_ID * 100 + 1, AWAY_ID, 9, with_optional_regime, played=False),
        ],
        "status": "closed",
        "home_team_side": ["left_to_right", "right_to_left"],
        "ball": {"trackable_object": 55},
        "pitch_length": 105,
        "pitch_width": 68,
    }


MATCHES_JSON = [
    {
        "id": MATCH_A,
        "date_time": "2024-12-21T06:00:00Z",
        "home_team": {"id": HOME_ID, "short_name": "Brisbane FC"},
        "away_team": {"id": AWAY_ID, "short_name": "Perth Glory"},
        "status": "closed",
        "competition_id": 61,
        "season_id": 95,
        "competition_edition_id": 870,
    },
    {
        "id": MATCH_B,
        "date_time": "2024-12-22T06:00:00Z",
        "home_team": {"id": AWAY_ID, "short_name": "Perth Glory"},
        "away_team": {"id": HOME_ID, "short_name": "Brisbane FC"},
        "status": "closed",
        "competition_id": 61,
        "season_id": 95,
        "competition_edition_id": 870,
    },
]


def write_json(path: Path, data: Any) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(data).encode("utf-8")
    path.write_bytes(blob)
    return blob


def entry(path: str, blob: bytes, role: str, match_id: int | None) -> FileEntry:
    return FileEntry(
        path=path,
        bytes=len(blob),
        sha256=hashlib.sha256(blob).hexdigest(),
        lfs=False,
        role=role,
        match_id=match_id,
    )


def frozen_source(files: list[FileEntry]) -> Source:
    repo = "SkillCorner/opendata"
    return Source(
        source_id=SOURCE_ID,
        sport="football",
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
        "verified": 3,
        "downloaded": 0,
        "failed": [],
        "finished_at": "2026-09-22T10:00:00+00:00",
        "source_path": str(source_path),
    }
    (receipt_dir / RECEIPT_NAME).write_text(json.dumps(receipt), encoding="utf-8")


@pytest.fixture
def bundle(tmp_path: Path) -> tuple[Path, Path]:
    """(manifest path, data dir): 2 matches' match.json (the two regimes) + matches.json."""
    clone = tmp_path / "clone"
    data_dir = tmp_path / "data"
    entries = []
    for match_id, with_optional in ((MATCH_A, True), (MATCH_B, False)):
        blob = write_json(clone / match_json_path(match_id), match_json(match_id, with_optional))
        entries.append(entry(match_json_path(match_id), blob, "match", match_id))
    matches_blob = write_json(clone / "data" / "matches.json", MATCHES_JSON)
    entries.append(entry("data/matches.json", matches_blob, "matches", None))

    manifest_path = tmp_path / "sources.json"
    save_manifest(Manifest(schema_version=1, sources=[frozen_source(entries)]), manifest_path)
    write_receipt(data_dir, clone)
    return manifest_path, data_dir


def rows(path: Path) -> list[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


def run_meta(manifest_path: Path, data_dir: Path) -> int:
    return main(
        [
            "bronze",
            "football",
            "--tables",
            "meta",
            "--manifest",
            str(manifest_path),
            "--data-dir",
            str(data_dir),
        ]
    )


def test_match_one_row_with_flattened_header(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    assert run_meta(manifest_path, data_dir) == 0

    match = rows(bronze_path(data_dir, "football", "match", MATCH_A))
    assert len(match) == 1
    row = match[0]
    assert (row["sport"], row["match_id"], row["source_row"]) == ("football", MATCH_A, 0)
    assert row["id"] == MATCH_A
    assert row["stadium_id"] == 2914
    assert row["stadium_capacity"] == 11507
    assert row["home_team_id"] == HOME_ID
    assert row["home_team_kit_season_start_year"] == 2020
    assert row["away_team_kit_jersey_color"] == "#ffffff"
    assert row["home_team_coach"] is None
    assert row["competition_edition_competition_area"] == "AUS"
    assert row["competition_edition_season_name"] == "2024/2025"
    assert row["competition_round_round_number"] == 9
    assert row["home_team_side"] == ["left_to_right", "right_to_left"]
    assert row["ball_trackable_object"] == 55
    assert row["pitch_length"] == 105


def test_match_periods_two_rows_per_match(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    run_meta(manifest_path, data_dir)

    periods = rows(bronze_path(data_dir, "football", "match_periods", MATCH_A))
    assert len(periods) == 2
    assert [p["period"] for p in periods] == [1, 2]
    assert [p["start_frame"] for p in periods] == [10, 30250]
    assert [p["end_frame"] for p in periods] == [30240, 61300]
    assert [p["source_row"] for p in periods] == [0, 1]
    assert all(p["match_id"] == MATCH_A for p in periods)


def test_match_players_two_regimes_and_unplayed_player(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    run_meta(manifest_path, data_dir)

    players_a = rows(bronze_path(data_dir, "football", "match_players", MATCH_A))
    assert len(players_a) == 2
    played, unplayed = players_a
    assert played["last_modified"] == "2026-04-01T13:19:03.072753Z", "regime with last_modified"
    assert unplayed["last_modified"] == "2026-04-01T13:19:03.072753Z"
    assert played["playing_time_sequences"] == [
        {"period": "period_1", "start_frame": 10, "end_frame": 19869}
    ]
    assert unplayed["playing_time_sequences"] == [], "regime has the key, but this player has none"
    assert played["playing_time_total_minutes_tip"] == 10.28
    assert played["playing_time_by_period"] == [
        {
            "name": "period_1",
            "minutes_tip": 10.28,
            "minutes_otip": 8.12,
            "start_frame": 10,
            "end_frame": 19869,
            "minutes_played": 33.1,
        }
    ]
    assert unplayed["playing_time_total_minutes_tip"] is None, "never played: all-NULL, not zero"
    assert unplayed["playing_time_total_start_frame"] is None
    assert unplayed["playing_time_by_period"] == []
    assert unplayed["start_time"] is None
    assert played["player_role_id"] == 3
    assert played["position_group"] == "Central Defender"
    assert played["player_role_acronym"] == "LCB"

    players_b = rows(bronze_path(data_dir, "football", "match_players", MATCH_B))
    assert players_b[0]["last_modified"] is None, "regime without last_modified at all"
    assert players_b[0]["playing_time_sequences"] is None, "key absent, not an empty list"
    assert players_b[1]["playing_time_sequences"] is None


def test_matches_is_one_season_file_flattening_teams(bundle: tuple[Path, Path]) -> None:
    manifest_path, data_dir = bundle
    run_meta(manifest_path, data_dir)

    matches = rows(bronze_season_path(data_dir, "football", "matches"))
    assert len(matches) == 2
    assert [m["match_id"] for m in matches] == [MATCH_A, MATCH_B]
    assert matches[0]["home_team_id"] == HOME_ID
    assert matches[0]["home_team_short_name"] == "Brisbane FC"
    assert matches[0]["status"] == "closed"
    assert [m["source_row"] for m in matches] == [0, 1]


def test_match_id_mismatch_fails_naming_the_file(tmp_path: Path) -> None:
    raw_root = tmp_path / "clone"
    data_dir = tmp_path / "data"
    bad = match_json(MATCH_A, with_optional_regime=False)
    bad["id"] = 999999
    blob = write_json(raw_root / match_json_path(MATCH_A), bad)
    source = frozen_source([entry(match_json_path(MATCH_A), blob, "match", MATCH_A)])

    def log(_: str) -> None:
        pass

    with pytest.raises(ValueError, match=r"id 999999 != 8100001"):
        build_match_tables(source, raw_root, data_dir, COMMIT, log)
    assert not (data_dir / "bronze").exists(), "nothing written before the failing check"


# --- key-set imposition: every nested dict is checked before a value is read ---------


def test_check_keys_reports_extra_and_missing() -> None:
    with pytest.raises(ValueError, match=r"unexpected=\['extra'\] missing=\['id'\]"):
        _check_keys({"extra", "name"}, [frozenset({"id", "name"})], "ctx")


def test_match_row_rejects_an_unknown_top_level_key() -> None:
    m = match_json(MATCH_A, with_optional_regime=False)
    m["unexpected_field"] = 1
    with pytest.raises(ValueError, match=r"match: keys .* unexpected=\['unexpected_field'\]"):
        _match_row(m, match_json_path(MATCH_A))


def test_match_row_rejects_a_missing_nested_key() -> None:
    m = match_json(MATCH_A, with_optional_regime=False)
    del m["stadium"]["capacity"]
    with pytest.raises(ValueError, match=r"match\.stadium.*missing=\['capacity'\]"):
        _match_row(m, match_json_path(MATCH_A))


def test_player_columns_rejects_an_unknown_player_key_set() -> None:
    players = [_player(HOME_ID * 100 + 1, HOME_ID, 4, with_optional=False, played=True)]
    del players[0]["gender"]
    with pytest.raises(ValueError, match=r"players\[0\].*missing=\['gender'\]"):
        _player_columns(players, match_json_path(MATCH_A))


def test_player_columns_rejects_an_unknown_playing_time_key_set() -> None:
    players = [_player(HOME_ID * 100 + 1, HOME_ID, 4, with_optional=False, played=True)]
    players[0]["playing_time"]["extra"] = 1
    with pytest.raises(ValueError, match=r"playing_time.*unexpected=\['extra'\]"):
        _player_columns(players, match_json_path(MATCH_A))


def test_period_columns_rejects_a_bad_period_shape() -> None:
    periods = [{"period": 1, "name": "period_1", "start_frame": 0, "end_frame": 10}]
    with pytest.raises(ValueError, match=r"match_periods\[0\].*missing="):
        _period_columns(periods, match_json_path(MATCH_A))
