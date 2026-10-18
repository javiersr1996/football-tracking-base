"""Bronze of the football metadata: ``match``, ``match_periods``, ``match_players``,
``matches``.

``match.json`` is read once per match and split into three tables at three grains: one
row of ``match`` (the flat header), one row of ``match_periods`` per period (2 per
match, 40 across the 20) and one row of ``match_players`` per roster entry (35-36 per
match). ``matches`` is the one exception to per-match files, exactly as in
``bronze/basketball/meta.py``: ``data/matches.json`` already holds all 20 matches
together, so bronze keeps it as a single season file while every row still carries its
own ``match_id`` (the native ``id``) in ``LINEAGE_FIELDS``.

**Every nested dict and list of dicts read from ``match.json`` has its key set imposed
against the set measured over the 20 matches, before any value is read from it**
(``_check_keys``), the same discipline ``bronze/basketball/tracking.py`` applies to
``FRAME_KEYS``/``PLAYER_KEYS`` and that task 6 paid for learning: a column built from
the schema instead of from the raw row lets a provider key silently disappear. A key
missing or a key extra fails the run naming the file, the path inside the JSON, and the
keys that differ.

Two things measured while writing this module that ``docs/formats/football.md`` (task
4) does not mention, because task 4 probed frames and CSVs, not ``match.json``:

- **``players[]`` has two regimes, split by match, not by player.** 10 of the 20
  matches carry ``last_modified`` (a per-player processing timestamp) and
  ``playing_time.sequences`` (a list of ``{period, start_frame, end_frame}``, the
  actual on-pitch windows, a finer grain than ``playing_time.by_period``); the other
  10 have neither key. Uniform within a match, never mixed. Both are optional columns:
  ``last_modified`` is NULL and ``playing_time_sequences`` is NULL (not an empty list,
  which is a delivered value in the regime that has the key) when the match's regime
  omits them.
- **``playing_time.total`` is NULL for a player who never played** (``start_time`` and
  ``end_time`` both null, ``playing_time.by_period`` an empty list): 96 such players
  across the 20 matches. Bronze keeps ``playing_time_total_*`` all-NULL for that row
  rather than inventing zeros.

``referees`` is an empty list in all 20 matches (and ``home_team_coach``/
``away_team_coach`` are always null): bronze carries a nullable ``home_team_coach``/
``away_team_coach`` column (the leaf exists, its value is always null) but no column for
``referees`` (the list itself is a leaf with zero scalar leaves under it in every match
seen; if the provider ever populates it, the per-match leaf checker in
``tests/contracts/test_football_meta_bronze.py`` catches the new, unrepresented path).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from tracklab.bronze.common import (
    LINEAGE_FIELDS,
    ParquetSink,
    batch_from_columns,
    bronze_path,
    bronze_season_path,
)
from tracklab.ingest.manifest import FileEntry, Source

SPORT = "football"
Log = Callable[[str], None]

# --- key sets measured over the 20 matches of manifest.commit 4340d274... -----------

MATCH_KEYS = frozenset(
    {
        "id",
        "home_team_score",
        "away_team_score",
        "date_time",
        "stadium",
        "home_team",
        "home_team_kit",
        "away_team",
        "away_team_kit",
        "home_team_coach",
        "away_team_coach",
        "home_team_playing_time",
        "away_team_playing_time",
        "competition_edition",
        "match_periods",
        "competition_round",
        "referees",
        "players",
        "status",
        "home_team_side",
        "ball",
        "pitch_length",
        "pitch_width",
    }
)
STADIUM_KEYS = frozenset({"id", "name", "city", "capacity"})
TEAM_KEYS = frozenset({"id", "name", "short_name", "acronym"})
KIT_KEYS = frozenset({"id", "team_id", "season", "name", "jersey_color", "number_color"})
KIT_SEASON_KEYS = frozenset({"id", "start_year", "end_year", "name"})
TEAM_PLAYING_TIME_KEYS = frozenset({"minutes_tip", "minutes_otip"})
COMPETITION_EDITION_KEYS = frozenset({"id", "competition", "season", "name"})
COMPETITION_KEYS = frozenset({"id", "area", "name", "gender", "age_group"})
COMPETITION_ROUND_KEYS = frozenset({"id", "name", "round_number", "potential_overtime"})
MATCH_BALL_KEYS = frozenset({"trackable_object"})
PERIOD_KEYS = frozenset(
    {"period", "name", "start_frame", "end_frame", "duration_frames", "duration_minutes"}
)
PLAYER_ROLE_KEYS = frozenset({"id", "position_group", "name", "acronym"})
# Two regimes, split by match: see the module docstring.
PLAYER_KEYS_BASE = frozenset(
    {
        "birthday",
        "end_time",
        "first_name",
        "gender",
        "goal",
        "id",
        "injured",
        "last_name",
        "number",
        "own_goal",
        "player_role",
        "playing_time",
        "red_card",
        "short_name",
        "start_time",
        "team_id",
        "team_player_id",
        "trackable_object",
        "yellow_card",
    }
)
PLAYER_KEYS_WITH_LAST_MODIFIED = PLAYER_KEYS_BASE | {"last_modified"}
PLAYING_TIME_KEYS_BASE = frozenset({"by_period", "total"})
PLAYING_TIME_KEYS_WITH_SEQUENCES = PLAYING_TIME_KEYS_BASE | {"sequences"}
PLAYING_TIME_TOTAL_KEYS = frozenset(
    {
        "minutes_tip",
        "minutes_otip",
        "start_frame",
        "end_frame",
        "minutes_played",
        "minutes_played_regular_time",
    }
)
BY_PERIOD_ENTRY_KEYS = frozenset(
    {"name", "minutes_tip", "minutes_otip", "start_frame", "end_frame", "minutes_played"}
)
SEQUENCE_ENTRY_KEYS = frozenset({"period", "start_frame", "end_frame"})
MATCHES_ENTRY_KEYS = frozenset(
    {
        "id",
        "date_time",
        "home_team",
        "away_team",
        "status",
        "competition_id",
        "season_id",
        "competition_edition_id",
    }
)
MATCHES_TEAM_KEYS = frozenset({"id", "short_name"})


def _check_keys(observed: Iterable[str], allowed: list[frozenset[str]], context: str) -> None:
    """Fail with ``context``, the keys observed and the closest allowed set's diff."""
    obs = frozenset(observed)
    if obs in allowed:
        return
    closest = min(allowed, key=lambda a: len(obs ^ a))
    extra = sorted(obs - closest)
    missing = sorted(closest - obs)
    raise ValueError(f"{context}: keys {sorted(obs)} unexpected={extra} missing={missing}")


# --- schemas --------------------------------------------------------------------------

BY_PERIOD_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("minutes_tip", pa.float64(), nullable=False),
        pa.field("minutes_otip", pa.float64(), nullable=False),
        pa.field("start_frame", pa.int64(), nullable=False),
        pa.field("end_frame", pa.int64(), nullable=False),
        pa.field("minutes_played", pa.float64(), nullable=False),
    ]
)
SEQUENCE_STRUCT = pa.struct(
    [
        pa.field("period", pa.string(), nullable=False),
        pa.field("start_frame", pa.int64(), nullable=False),
        pa.field("end_frame", pa.int64(), nullable=False),
    ]
)

MATCH_SCHEMA = pa.schema(
    [
        *LINEAGE_FIELDS,
        pa.field("id", pa.int64(), nullable=False),
        pa.field("home_team_score", pa.int64(), nullable=False),
        pa.field("away_team_score", pa.int64(), nullable=False),
        pa.field("date_time", pa.string(), nullable=False),
        pa.field("stadium_id", pa.int64(), nullable=False),
        pa.field("stadium_name", pa.string(), nullable=False),
        pa.field("stadium_city", pa.string(), nullable=False),
        pa.field("stadium_capacity", pa.int64()),  # null in some matches
        pa.field("home_team_id", pa.int64(), nullable=False),
        pa.field("home_team_name", pa.string(), nullable=False),
        pa.field("home_team_short_name", pa.string(), nullable=False),
        pa.field("home_team_acronym", pa.string(), nullable=False),
        pa.field("away_team_id", pa.int64(), nullable=False),
        pa.field("away_team_name", pa.string(), nullable=False),
        pa.field("away_team_short_name", pa.string(), nullable=False),
        pa.field("away_team_acronym", pa.string(), nullable=False),
        pa.field("home_team_kit_id", pa.int64(), nullable=False),
        pa.field("home_team_kit_team_id", pa.int64(), nullable=False),
        pa.field("home_team_kit_name", pa.string(), nullable=False),
        pa.field("home_team_kit_jersey_color", pa.string(), nullable=False),
        pa.field("home_team_kit_number_color", pa.string(), nullable=False),
        pa.field("home_team_kit_season_id", pa.int64(), nullable=False),
        pa.field("home_team_kit_season_start_year", pa.int64(), nullable=False),
        pa.field("home_team_kit_season_end_year", pa.int64(), nullable=False),
        pa.field("home_team_kit_season_name", pa.string(), nullable=False),
        pa.field("away_team_kit_id", pa.int64(), nullable=False),
        pa.field("away_team_kit_team_id", pa.int64(), nullable=False),
        pa.field("away_team_kit_name", pa.string(), nullable=False),
        pa.field("away_team_kit_jersey_color", pa.string(), nullable=False),
        pa.field("away_team_kit_number_color", pa.string(), nullable=False),
        pa.field("away_team_kit_season_id", pa.int64(), nullable=False),
        pa.field("away_team_kit_season_start_year", pa.int64(), nullable=False),
        pa.field("away_team_kit_season_end_year", pa.int64(), nullable=False),
        pa.field("away_team_kit_season_name", pa.string(), nullable=False),
        # Always null in the 20 matches; typed on the one shape ever observed (see docstring).
        pa.field("home_team_coach", pa.string()),
        pa.field("away_team_coach", pa.string()),
        pa.field("home_team_playing_time_minutes_tip", pa.float64(), nullable=False),
        pa.field("home_team_playing_time_minutes_otip", pa.float64(), nullable=False),
        pa.field("away_team_playing_time_minutes_tip", pa.float64(), nullable=False),
        pa.field("away_team_playing_time_minutes_otip", pa.float64(), nullable=False),
        pa.field("competition_edition_id", pa.int64(), nullable=False),
        pa.field("competition_edition_name", pa.string(), nullable=False),
        pa.field("competition_edition_competition_id", pa.int64(), nullable=False),
        pa.field("competition_edition_competition_area", pa.string(), nullable=False),
        pa.field("competition_edition_competition_name", pa.string(), nullable=False),
        pa.field("competition_edition_competition_gender", pa.string(), nullable=False),
        pa.field("competition_edition_competition_age_group", pa.string(), nullable=False),
        pa.field("competition_edition_season_id", pa.int64(), nullable=False),
        pa.field("competition_edition_season_start_year", pa.int64(), nullable=False),
        pa.field("competition_edition_season_end_year", pa.int64(), nullable=False),
        pa.field("competition_edition_season_name", pa.string(), nullable=False),
        pa.field("competition_round_id", pa.int64(), nullable=False),
        pa.field("competition_round_name", pa.string(), nullable=False),
        pa.field("competition_round_round_number", pa.int64(), nullable=False),
        pa.field("competition_round_potential_overtime", pa.bool_(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("home_team_side", pa.list_(pa.string()), nullable=False),  # one entry per period
        pa.field("ball_trackable_object", pa.int64(), nullable=False),
        pa.field("pitch_length", pa.int64(), nullable=False),
        pa.field("pitch_width", pa.int64(), nullable=False),
    ]
)

MATCH_PERIODS_SCHEMA = pa.schema(
    [
        *LINEAGE_FIELDS,
        pa.field("period", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("start_frame", pa.int64(), nullable=False),
        pa.field("end_frame", pa.int64(), nullable=False),
        pa.field("duration_frames", pa.int64(), nullable=False),
        pa.field("duration_minutes", pa.float64(), nullable=False),
    ]
)

MATCH_PLAYERS_SCHEMA = pa.schema(
    [
        *LINEAGE_FIELDS,
        pa.field("id", pa.int64(), nullable=False),
        pa.field("team_id", pa.int64(), nullable=False),
        pa.field("team_player_id", pa.int64(), nullable=False),
        pa.field("trackable_object", pa.int64(), nullable=False),
        pa.field("first_name", pa.string(), nullable=False),
        pa.field("last_name", pa.string(), nullable=False),
        pa.field("short_name", pa.string(), nullable=False),
        pa.field("birthday", pa.string()),  # null: player has no birthday on file
        pa.field("gender", pa.string(), nullable=False),
        pa.field("number", pa.int64(), nullable=False),
        pa.field("start_time", pa.string()),  # null: never took the pitch
        pa.field("end_time", pa.string()),
        pa.field("yellow_card", pa.int64(), nullable=False),
        pa.field("red_card", pa.int64(), nullable=False),
        pa.field("injured", pa.bool_(), nullable=False),
        pa.field("goal", pa.int64(), nullable=False),
        pa.field("own_goal", pa.int64(), nullable=False),
        pa.field("last_modified", pa.string()),  # null in 10 of 20 matches: see docstring
        pa.field("player_role_id", pa.int64(), nullable=False),
        pa.field("position_group", pa.string(), nullable=False),
        pa.field("player_role_name", pa.string(), nullable=False),
        pa.field("player_role_acronym", pa.string(), nullable=False),
        # null in every field together: the player never played (96 rows across 20 matches)
        pa.field("playing_time_total_minutes_tip", pa.float64()),
        pa.field("playing_time_total_minutes_otip", pa.float64()),
        pa.field("playing_time_total_start_frame", pa.int64()),
        pa.field("playing_time_total_end_frame", pa.int64()),
        pa.field("playing_time_total_minutes_played", pa.float64()),
        pa.field("playing_time_total_minutes_played_regular_time", pa.float64()),
        pa.field("playing_time_by_period", pa.list_(BY_PERIOD_STRUCT), nullable=False),
        # null (not []) when the match's regime omits `sequences` entirely: see docstring
        pa.field("playing_time_sequences", pa.list_(SEQUENCE_STRUCT)),
    ]
)

MATCHES_SCHEMA = pa.schema(
    [
        *LINEAGE_FIELDS,
        pa.field("id", pa.int64(), nullable=False),
        pa.field("date_time", pa.string(), nullable=False),
        pa.field("home_team_id", pa.int64(), nullable=False),
        pa.field("home_team_short_name", pa.string(), nullable=False),
        pa.field("away_team_id", pa.int64(), nullable=False),
        pa.field("away_team_short_name", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("competition_id", pa.int64(), nullable=False),
        pa.field("season_id", pa.int64(), nullable=False),
        pa.field("competition_edition_id", pa.int64(), nullable=False),
    ]
)

TABLES = {
    "match": MATCH_SCHEMA,
    "match_periods": MATCH_PERIODS_SCHEMA,
    "match_players": MATCH_PLAYERS_SCHEMA,
    "matches": MATCHES_SCHEMA,
}


@dataclass(frozen=True)
class MetaStats:
    table: str
    match_id: int | None
    rows: int
    seconds: float
    mb: float


def _log_stat(log: Log, s: MetaStats) -> None:
    match_id = s.match_id if s.match_id is not None else "-"
    log(f"{s.table:>17} {match_id:>8} {s.rows:>6} {s.seconds:>8.2f} {s.mb:>7.2f}")


def _only_file(source: Source, role: str) -> FileEntry:
    matches = [f for f in source.files if f.role == role and f.match_id is None]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one '{role}' file, found {len(matches)}")
    return matches[0]


# --- match.json -> match (1 row) -------------------------------------------------------


def _match_row(m: Mapping[str, Any], source_file: str) -> dict[str, Any]:
    _check_keys(m.keys(), [MATCH_KEYS], f"{source_file}: match")
    stadium, home, away = m["stadium"], m["home_team"], m["away_team"]
    home_kit, away_kit = m["home_team_kit"], m["away_team_kit"]
    home_ptime, away_ptime = m["home_team_playing_time"], m["away_team_playing_time"]
    ce, cr, ball = m["competition_edition"], m["competition_round"], m["ball"]
    _check_keys(stadium.keys(), [STADIUM_KEYS], f"{source_file}: match.stadium")
    _check_keys(home.keys(), [TEAM_KEYS], f"{source_file}: match.home_team")
    _check_keys(away.keys(), [TEAM_KEYS], f"{source_file}: match.away_team")
    _check_keys(home_kit.keys(), [KIT_KEYS], f"{source_file}: match.home_team_kit")
    _check_keys(
        home_kit["season"].keys(), [KIT_SEASON_KEYS], f"{source_file}: home_team_kit.season"
    )
    _check_keys(away_kit.keys(), [KIT_KEYS], f"{source_file}: match.away_team_kit")
    _check_keys(
        away_kit["season"].keys(), [KIT_SEASON_KEYS], f"{source_file}: away_team_kit.season"
    )
    _check_keys(
        home_ptime.keys(), [TEAM_PLAYING_TIME_KEYS], f"{source_file}: home_team_playing_time"
    )
    _check_keys(
        away_ptime.keys(), [TEAM_PLAYING_TIME_KEYS], f"{source_file}: away_team_playing_time"
    )
    _check_keys(ce.keys(), [COMPETITION_EDITION_KEYS], f"{source_file}: competition_edition")
    _check_keys(
        ce["competition"].keys(),
        [COMPETITION_KEYS],
        f"{source_file}: competition_edition.competition",
    )
    _check_keys(
        ce["season"].keys(), [KIT_SEASON_KEYS], f"{source_file}: competition_edition.season"
    )
    _check_keys(cr.keys(), [COMPETITION_ROUND_KEYS], f"{source_file}: competition_round")
    _check_keys(ball.keys(), [MATCH_BALL_KEYS], f"{source_file}: match.ball")

    return {
        "id": m["id"],
        "home_team_score": m["home_team_score"],
        "away_team_score": m["away_team_score"],
        "date_time": m["date_time"],
        "stadium_id": stadium["id"],
        "stadium_name": stadium["name"],
        "stadium_city": stadium["city"],
        "stadium_capacity": stadium["capacity"],
        "home_team_id": home["id"],
        "home_team_name": home["name"],
        "home_team_short_name": home["short_name"],
        "home_team_acronym": home["acronym"],
        "away_team_id": away["id"],
        "away_team_name": away["name"],
        "away_team_short_name": away["short_name"],
        "away_team_acronym": away["acronym"],
        "home_team_kit_id": home_kit["id"],
        "home_team_kit_team_id": home_kit["team_id"],
        "home_team_kit_name": home_kit["name"],
        "home_team_kit_jersey_color": home_kit["jersey_color"],
        "home_team_kit_number_color": home_kit["number_color"],
        "home_team_kit_season_id": home_kit["season"]["id"],
        "home_team_kit_season_start_year": home_kit["season"]["start_year"],
        "home_team_kit_season_end_year": home_kit["season"]["end_year"],
        "home_team_kit_season_name": home_kit["season"]["name"],
        "away_team_kit_id": away_kit["id"],
        "away_team_kit_team_id": away_kit["team_id"],
        "away_team_kit_name": away_kit["name"],
        "away_team_kit_jersey_color": away_kit["jersey_color"],
        "away_team_kit_number_color": away_kit["number_color"],
        "away_team_kit_season_id": away_kit["season"]["id"],
        "away_team_kit_season_start_year": away_kit["season"]["start_year"],
        "away_team_kit_season_end_year": away_kit["season"]["end_year"],
        "away_team_kit_season_name": away_kit["season"]["name"],
        "home_team_coach": m["home_team_coach"],
        "away_team_coach": m["away_team_coach"],
        "home_team_playing_time_minutes_tip": home_ptime["minutes_tip"],
        "home_team_playing_time_minutes_otip": home_ptime["minutes_otip"],
        "away_team_playing_time_minutes_tip": away_ptime["minutes_tip"],
        "away_team_playing_time_minutes_otip": away_ptime["minutes_otip"],
        "competition_edition_id": ce["id"],
        "competition_edition_name": ce["name"],
        "competition_edition_competition_id": ce["competition"]["id"],
        "competition_edition_competition_area": ce["competition"]["area"],
        "competition_edition_competition_name": ce["competition"]["name"],
        "competition_edition_competition_gender": ce["competition"]["gender"],
        "competition_edition_competition_age_group": ce["competition"]["age_group"],
        "competition_edition_season_id": ce["season"]["id"],
        "competition_edition_season_start_year": ce["season"]["start_year"],
        "competition_edition_season_end_year": ce["season"]["end_year"],
        "competition_edition_season_name": ce["season"]["name"],
        "competition_round_id": cr["id"],
        "competition_round_name": cr["name"],
        "competition_round_round_number": cr["round_number"],
        "competition_round_potential_overtime": cr["potential_overtime"],
        "status": m["status"],
        "home_team_side": list(m["home_team_side"]),
        "ball_trackable_object": ball["trackable_object"],
        "pitch_length": m["pitch_length"],
        "pitch_width": m["pitch_width"],
    }


# --- match.json.match_periods[] -> match_periods (2 rows) ------------------------------


def _period_columns(periods: list[Mapping[str, Any]], source_file: str) -> dict[str, list[Any]]:
    for i, p in enumerate(periods):
        _check_keys(p.keys(), [PERIOD_KEYS], f"{source_file}: match_periods[{i}]")
    return {
        "period": [p["period"] for p in periods],
        "name": [p["name"] for p in periods],
        "start_frame": [p["start_frame"] for p in periods],
        "end_frame": [p["end_frame"] for p in periods],
        "duration_frames": [p["duration_frames"] for p in periods],
        "duration_minutes": [p["duration_minutes"] for p in periods],
    }


# --- match.json.players[] -> match_players (35-36 rows) --------------------------------

_TOTAL_NULL_COLUMNS = (
    "playing_time_total_minutes_tip",
    "playing_time_total_minutes_otip",
    "playing_time_total_start_frame",
    "playing_time_total_end_frame",
    "playing_time_total_minutes_played",
    "playing_time_total_minutes_played_regular_time",
)


def _player_columns(players: list[Mapping[str, Any]], source_file: str) -> dict[str, list[Any]]:
    columns: dict[str, list[Any]] = {name: [] for name in MATCH_PLAYERS_SCHEMA.names[4:]}
    for i, p in enumerate(players):
        ctx = f"{source_file}: players[{i}]"
        _check_keys(p.keys(), [PLAYER_KEYS_BASE, PLAYER_KEYS_WITH_LAST_MODIFIED], ctx)
        role = p["player_role"]
        _check_keys(role.keys(), [PLAYER_ROLE_KEYS], f"{ctx}.player_role")
        pt = p["playing_time"]
        _check_keys(
            pt.keys(),
            [PLAYING_TIME_KEYS_BASE, PLAYING_TIME_KEYS_WITH_SEQUENCES],
            f"{ctx}.playing_time",
        )
        total = pt["total"]
        if total is not None:
            _check_keys(total.keys(), [PLAYING_TIME_TOTAL_KEYS], f"{ctx}.playing_time.total")
        for j, bp in enumerate(pt["by_period"]):
            _check_keys(bp.keys(), [BY_PERIOD_ENTRY_KEYS], f"{ctx}.playing_time.by_period[{j}]")
        sequences = pt.get("sequences")
        if sequences is not None:
            for j, sq in enumerate(sequences):
                _check_keys(sq.keys(), [SEQUENCE_ENTRY_KEYS], f"{ctx}.playing_time.sequences[{j}]")

        columns["id"].append(p["id"])
        columns["team_id"].append(p["team_id"])
        columns["team_player_id"].append(p["team_player_id"])
        columns["trackable_object"].append(p["trackable_object"])
        columns["first_name"].append(p["first_name"])
        columns["last_name"].append(p["last_name"])
        columns["short_name"].append(p["short_name"])
        columns["birthday"].append(p["birthday"])
        columns["gender"].append(p["gender"])
        columns["number"].append(p["number"])
        columns["start_time"].append(p["start_time"])
        columns["end_time"].append(p["end_time"])
        columns["yellow_card"].append(p["yellow_card"])
        columns["red_card"].append(p["red_card"])
        columns["injured"].append(p["injured"])
        columns["goal"].append(p["goal"])
        columns["own_goal"].append(p["own_goal"])
        columns["last_modified"].append(p.get("last_modified"))
        columns["player_role_id"].append(role["id"])
        columns["position_group"].append(role["position_group"])
        columns["player_role_name"].append(role["name"])
        columns["player_role_acronym"].append(role["acronym"])
        if total is None:
            for name in _TOTAL_NULL_COLUMNS:
                columns[name].append(None)
        else:
            columns["playing_time_total_minutes_tip"].append(total["minutes_tip"])
            columns["playing_time_total_minutes_otip"].append(total["minutes_otip"])
            columns["playing_time_total_start_frame"].append(total["start_frame"])
            columns["playing_time_total_end_frame"].append(total["end_frame"])
            columns["playing_time_total_minutes_played"].append(total["minutes_played"])
            columns["playing_time_total_minutes_played_regular_time"].append(
                total["minutes_played_regular_time"]
            )
        columns["playing_time_by_period"].append(
            [
                {
                    "name": bp["name"],
                    "minutes_tip": bp["minutes_tip"],
                    "minutes_otip": bp["minutes_otip"],
                    "start_frame": bp["start_frame"],
                    "end_frame": bp["end_frame"],
                    "minutes_played": bp["minutes_played"],
                }
                for bp in pt["by_period"]
            ]
        )
        columns["playing_time_sequences"].append(
            None
            if sequences is None
            else [
                {
                    "period": sq["period"],
                    "start_frame": sq["start_frame"],
                    "end_frame": sq["end_frame"],
                }
                for sq in sequences
            ]
        )
    return columns


def build_match_tables(
    source: Source, raw_root: Path, data_dir: Path, commit: str, log: Log
) -> list[MetaStats]:
    """``match``, ``match_periods`` and ``match_players``, one Parquet per match and table."""
    entries = sorted(
        (f for f in source.files if f.role == "match" and f.match_id is not None),
        key=lambda f: f.match_id or 0,
    )
    stats: list[MetaStats] = []
    for entry in entries:
        assert entry.match_id is not None
        started = time.perf_counter()
        m = json.loads((raw_root / entry.path).read_text(encoding="utf-8"))
        if m["id"] != entry.match_id:
            raise ValueError(f"{entry.path}: match.json id {m['id']} != {entry.match_id}")
        lineage = {"sport": SPORT, "match_id": entry.match_id, "source_file": entry.path}

        match_batch = batch_from_columns(
            MATCH_SCHEMA,
            {
                **lineage,
                "source_row": [0],
                **{k: [v] for k, v in _match_row(m, entry.path).items()},
            },
            1,
        )
        period_columns = _period_columns(m["match_periods"], entry.path)
        n_periods = len(m["match_periods"])
        periods_batch = batch_from_columns(
            MATCH_PERIODS_SCHEMA,
            {**lineage, "source_row": list(range(n_periods)), **period_columns},
            n_periods,
        )
        player_columns = _player_columns(m["players"], entry.path)
        n_players = len(m["players"])
        players_batch = batch_from_columns(
            MATCH_PLAYERS_SCHEMA,
            {**lineage, "source_row": list(range(n_players)), **player_columns},
            n_players,
        )

        metadata = {
            "tracklab.sport": SPORT,
            "tracklab.source_file": entry.path,
            "tracklab.source_sha256": entry.sha256,
            "tracklab.source_commit": commit,
        }
        match_path = bronze_path(data_dir, SPORT, "match", entry.match_id)
        periods_path = bronze_path(data_dir, SPORT, "match_periods", entry.match_id)
        players_path = bronze_path(data_dir, SPORT, "match_players", entry.match_id)
        with ParquetSink(match_path, MATCH_SCHEMA, metadata) as match_sink:
            match_sink.write(match_batch)
        with ParquetSink(periods_path, MATCH_PERIODS_SCHEMA, metadata) as periods_sink:
            periods_sink.write(periods_batch)
        with ParquetSink(players_path, MATCH_PLAYERS_SCHEMA, metadata) as players_sink:
            players_sink.write(players_batch)

        seconds = time.perf_counter() - started
        s_match = MetaStats(
            "match", entry.match_id, match_sink.rows, seconds, match_sink.bytes / 1e6
        )
        s_periods = MetaStats(
            "match_periods", entry.match_id, periods_sink.rows, seconds, periods_sink.bytes / 1e6
        )
        s_players = MetaStats(
            "match_players", entry.match_id, players_sink.rows, seconds, players_sink.bytes / 1e6
        )
        for s in (s_match, s_periods, s_players):
            _log_stat(log, s)
        stats += [s_match, s_periods, s_players]
    return stats


# --- data/matches.json -> matches (season file, 20 rows) -------------------------------


def build_matches(
    source: Source, raw_root: Path, data_dir: Path, commit: str, log: Log
) -> MetaStats:
    entry = _only_file(source, "matches")
    started = time.perf_counter()
    matches = json.loads((raw_root / entry.path).read_text(encoding="utf-8"))
    for i, row in enumerate(matches):
        _check_keys(row.keys(), [MATCHES_ENTRY_KEYS], f"{entry.path}: matches[{i}]")
        _check_keys(
            row["home_team"].keys(), [MATCHES_TEAM_KEYS], f"{entry.path}: matches[{i}].home_team"
        )
        _check_keys(
            row["away_team"].keys(), [MATCHES_TEAM_KEYS], f"{entry.path}: matches[{i}].away_team"
        )
    n = len(matches)
    columns: dict[str, Any] = {
        "sport": SPORT,
        "match_id": [m["id"] for m in matches],
        "source_file": entry.path,
        "source_row": list(range(n)),
        "id": [m["id"] for m in matches],
        "date_time": [m["date_time"] for m in matches],
        "home_team_id": [m["home_team"]["id"] for m in matches],
        "home_team_short_name": [m["home_team"]["short_name"] for m in matches],
        "away_team_id": [m["away_team"]["id"] for m in matches],
        "away_team_short_name": [m["away_team"]["short_name"] for m in matches],
        "status": [m["status"] for m in matches],
        "competition_id": [m["competition_id"] for m in matches],
        "season_id": [m["season_id"] for m in matches],
        "competition_edition_id": [m["competition_edition_id"] for m in matches],
    }
    batch = batch_from_columns(MATCHES_SCHEMA, columns, n)
    metadata = {
        "tracklab.sport": SPORT,
        "tracklab.source_file": entry.path,
        "tracklab.source_sha256": entry.sha256,
        "tracklab.source_commit": commit,
    }
    path = bronze_season_path(data_dir, SPORT, "matches")
    with ParquetSink(path, MATCHES_SCHEMA, metadata) as sink:
        sink.write(batch)
    s = MetaStats("matches", None, sink.rows, time.perf_counter() - started, sink.bytes / 1e6)
    _log_stat(log, s)
    return s


def build_meta(source: Source, raw_root: Path, data_dir: Path, log: Log = print) -> list[MetaStats]:
    """``match``, ``match_periods``, ``match_players`` and ``matches``, in that order."""
    commit = source.commit
    if commit is None:
        raise RuntimeError(f"{source.source_id} is not frozen; run `tracklab freeze` first")
    log(f"{'table':>17} {'match_id':>8} {'rows':>6} {'seconds':>8} {'MB':>7}")
    stats = build_match_tables(source, raw_root, data_dir, commit, log)
    stats.append(build_matches(source, raw_root, data_dir, commit, log))
    return stats
