# New in ftbase (no tracklab origin): the synthetic match the tests and the CI run on.
"""A short synthetic match in SkillCorner's raw format, with extrapolated runs planted.

``write_synthetic_match`` writes what ``ftbase ingest --from-existing``, ``ftbase bronze`` and
``ftbase silver`` read, with no network and no provider data: the five raw files of one match
(``match.json``, the tracking JSONL, the dynamic-events and phases-of-play CSVs with the
frozen headers, and ``matches.json``) plus a manifest whose sha256 values are those of the
bytes just written. So the whole pipeline runs on it unchanged, ingest included.

The match is built to be checked against a reference that does not come from the pipeline:

- 10 Hz, two periods (frames 0-59 and 70-129; frames 60-69 belong to no period, as the
  half-time footage does in the real bundle);
- home (team 7: players 71, 72, 73, and 74 who never plays) against away (team 8: players
  81, 82, 83); ``home_team_side`` is ``[left_to_right, right_to_left]``, so the teams
  change sides at half-time;
- ``is_detected`` is false exactly on the frames of the planted runs (:data:`PLANTED_RUNS`)
  and true everywhere else, for every player and for the ball. The writer returns the runs
  it planted, so a test compares what the pipeline recovers against them and never against
  the pipeline itself;
- six possessions and four phases of play, consistent with ``home_team_side``.

Nothing here is a measurement: coordinates come from a seeded ``random.Random`` and mean
nothing. No number computed on this match belongs in a report. Every file is written with
LF line endings, so a Linux checkout never sees a dirty tree.
"""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ftbase.bronze.football.events import load_frozen_tables
from ftbase.ingest.manifest import SCHEMA_VERSION

SOURCE_ID = "opendata-football"
FAKE_COMMIT = "0" * 40
HZ = 10
HOME_TEAM, AWAY_TEAM = 7, 8
HOME_PLAYERS, AWAY_PLAYERS = (71, 72, 73), (81, 82, 83)
BENCH_PLAYER = 74  # on the roster, never on the pitch
PERIODS: dict[int, tuple[int, int]] = {1: (0, 59), 2: (70, 129)}  # period -> (first, last) frame
HOME_TEAM_SIDE = ("left_to_right", "right_to_left")
TOTAL_FRAMES = 130
DEFAULT_MATCH_ID = 900001
DEFAULT_SEED = 20261018


@dataclass(frozen=True)
class PlantedRun:
    """``length`` consecutive frames of ``player_id`` with ``is_detected`` false in ``period``."""

    player_id: int
    period: int
    start_frame: int
    length: int

    @property
    def frames(self) -> range:
        return range(self.start_frame, self.start_frame + self.length)


# Two runs for one player in one period, runs that touch the end of a period (81 and 82) and
# the start of the next (81 and 83), and players with none (73): the shapes a run counter
# can get wrong.
PLANTED_RUNS: tuple[PlantedRun, ...] = (
    PlantedRun(71, 1, 5, 4),
    PlantedRun(71, 1, 30, 7),
    PlantedRun(72, 2, 80, 3),
    PlantedRun(81, 1, 50, 10),
    PlantedRun(81, 2, 70, 6),
    PlantedRun(82, 2, 125, 5),
    PlantedRun(83, 2, 70, 2),
)


@dataclass(frozen=True)
class SyntheticMatch:
    """Where a synthetic match was written and the reference it was written against."""

    root: Path  # raw bundle root: the folder that holds ``data/``
    manifest: Path  # sources.json for ``--manifest``; ``--from-existing`` takes ``root``
    match_id: int
    runs: tuple[PlantedRun, ...]
    home_team_side: tuple[str, ...]
    periods: dict[int, tuple[int, int]]  # period -> (first frame, last frame)


def _player_frames(run: PlantedRun) -> set[tuple[int, int]]:
    return {(run.player_id, f) for f in run.frames}


def _check_runs(runs: Sequence[PlantedRun]) -> None:
    players = {*HOME_PLAYERS, *AWAY_PLAYERS}
    seen: set[tuple[int, int]] = set()
    for run in runs:
        if run.player_id not in players:
            raise ValueError(f"{run}: player is not on the pitch (known: {sorted(players)})")
        if run.period not in PERIODS:
            raise ValueError(f"{run}: no period {run.period}")
        first, last = PERIODS[run.period]
        if run.length < 1 or run.start_frame < first or run.start_frame + run.length - 1 > last:
            raise ValueError(f"{run}: must lie inside frames {first}-{last} of its period")
        frames = _player_frames(run)
        if frames & seen:
            raise ValueError(f"{run}: overlaps another run of the same player")
        # Two runs of one player must not touch, or they would read back as one longer run.
        if any((p, f - 1) in seen or (p, f + 1) in seen for p, f in frames):
            raise ValueError(f"{run}: touches another run of the same player")
        seen |= frames


def _period_of(frame: int) -> int | None:
    for period, (first, last) in PERIODS.items():
        if first <= frame <= last:
            return period
    return None


def _timestamp(period: int, frame: int) -> str:
    tenths = ((period - 1) * 45 * 60 * HZ) + (frame - PERIODS[period][0])
    seconds, tenth = divmod(tenths, HZ)
    minutes, second = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    return f"{hours:02d}:{minute:02d}:{second:02d}.{tenth}0"


# --- tracking ----------------------------------------------------------------------------


def _tracking_lines(runs: Sequence[PlantedRun], seed: int) -> list[str]:
    rng = random.Random(seed)
    missing = {(p, f) for run in runs for p, f in _player_frames(run)}
    lines: list[str] = []
    for frame in range(TOTAL_FRAMES):
        period = _period_of(frame)
        if period is None:
            ball: dict[str, Any] = {"x": None, "y": None, "z": None, "is_detected": None}
            corners: dict[str, Any] = dict.fromkeys(_CORNERS)
            players: list[dict[str, Any]] = []
            possession: dict[str, Any] = {"player_id": None, "group": None}
            stamp = None
        else:
            ball = {
                "x": round(-20.0 + 0.4 * frame + rng.randint(-50, 50) / 100, 2),
                "y": round(rng.randint(-1000, 1000) / 100, 2),
                "z": round(rng.randint(0, 40) / 100, 2),
                "is_detected": True,
            }
            corners = {k: round(rng.randint(-5000, 5000) / 100, 2) for k in _CORNERS}
            players = []
            for i, player in enumerate((*HOME_PLAYERS, *AWAY_PLAYERS)):
                side = -1 if player in HOME_PLAYERS else 1
                players.append(
                    {
                        "player_id": player,
                        "x": round(
                            side * (8.0 + 6 * (i % 3)) + 0.1 * frame + rng.randint(-50, 50) / 100, 2
                        ),
                        "y": round(-20.0 + 12 * (i % 3) + rng.randint(-50, 50) / 100, 2),
                        "is_detected": (player, frame) not in missing,
                    }
                )
            holder = _POSSESSION_BY_FRAME.get(frame)
            possession = (
                {"player_id": None, "group": None}
                if holder is None
                else {"player_id": holder[0], "group": holder[1]}
            )
            stamp = _timestamp(period, frame)
        frame_row = {
            "frame": frame,
            "timestamp": stamp,
            "period": period,
            "ball_data": ball,
            "possession": possession,
            "image_corners_projection": corners,
            "player_data": players,
        }
        lines.append(json.dumps(frame_row))
    return lines


_CORNERS = (
    "x_top_left",
    "y_top_left",
    "x_bottom_left",
    "y_bottom_left",
    "x_bottom_right",
    "y_bottom_right",
    "x_top_right",
    "y_top_right",
)

# (player_in_possession, event team id, first frame, last frame, period, end_type, opens)
_POSSESSIONS: tuple[tuple[int, int, int, int, int, str, bool], ...] = (
    (71, HOME_TEAM, 0, 14, 1, "pass", True),
    (72, HOME_TEAM, 15, 29, 1, "possession_loss", False),
    (81, AWAY_TEAM, 30, 44, 1, "pass", True),
    (82, AWAY_TEAM, 45, 59, 1, "shot", False),
    (83, AWAY_TEAM, 70, 99, 2, "pass", True),
    (73, HOME_TEAM, 100, 129, 2, "possession_loss", True),
)
_POSSESSION_BY_FRAME: dict[int, tuple[int, str]] = {
    f: (p, "home team" if team == HOME_TEAM else "away team")
    for p, team, first, last, _, _, _ in _POSSESSIONS
    for f in range(first, last + 1)
}


def _attacking_side(team: int, period: int) -> str:
    home = HOME_TEAM_SIDE[period - 1]
    if team == HOME_TEAM:
        return home
    return "right_to_left" if home == "left_to_right" else "left_to_right"


# --- CSVs --------------------------------------------------------------------------------


def _cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    if any(c in text for c in ',"\r\n'):
        raise ValueError(f"fixture cell needs CSV quoting: {text!r}")
    return text


def _csv(names: Sequence[str], rows: Sequence[dict[str, object]]) -> str:
    unknown = {k for row in rows for k in row} - set(names)
    if unknown:
        raise ValueError(f"columns not in the frozen header: {sorted(unknown)}")
    lines = [",".join(names)]
    lines += [",".join(_cell(row.get(name)) for name in names) for row in rows]
    return "\n".join(lines) + "\n"


def _events_csv(match_id: int) -> str:
    names = load_frozen_tables()["dynamic_events"].names
    rows: list[dict[str, object]] = []
    for i, (player, team, first, last, period, end_type, opens) in enumerate(_POSSESSIONS):
        rows.append(
            {
                "event_id": f"e{i + 1}",
                "index": i,
                "match_id": match_id,
                "frame_start": first,
                "frame_end": last,
                "period": period,
                "attacking_side": _attacking_side(team, period),
                "event_type": "player_possession",
                "player_id": player,
                "team_id": team,
                "x_start": 10.0 + i,
                "y_start": 2.0,
                "x_end": 11.0 + i,
                "y_end": 3.0,
                "end_type": end_type,
                "first_player_possession_in_team_possession": opens,
            }
        )
    return _csv(names, rows)


def _phases_csv(match_id: int) -> str:
    names = load_frozen_tables()["phases_of_play"].names
    kinds = ("build_up", "transition", "create", "chaotic")
    rows: list[dict[str, object]] = []
    chains = [(0, 29, 1, HOME_TEAM), (30, 59, 1, AWAY_TEAM), (70, 99, 2, AWAY_TEAM),
              (100, 129, 2, HOME_TEAM)]  # fmt: skip
    for i, (first, last, period, team) in enumerate(chains):
        rows.append(
            {
                "index": i,
                "match_id": match_id,
                "frame_start": first,
                "frame_end": last,
                "period": period,
                "team_in_possession_id": team,
                "attacking_side": _attacking_side(team, period),
                "team_in_possession_phase_type": kinds[i],
                "team_possession_lead_to_goal": False,
                "team_possession_lead_to_shot": i == 1,
                "team_possession_loss_in_phase": i % 2 == 0,
                "x_start": 1.0 + i,
                "y_start": 0.0,
                "x_end": 2.0 + i,
                "y_end": 1.0,
            }
        )
    return _csv(names, rows)


# --- match.json and matches.json ----------------------------------------------------------


def _player_json(player: int, team: int) -> dict[str, Any]:
    plays = player != BENCH_PLAYER
    by_period: list[dict[str, Any]] = [
        {
            "name": f"period_{p}",
            "minutes_tip": 0.0,
            "minutes_otip": 0.0,
            "start_frame": first,
            "end_frame": last,
            "minutes_played": round((last - first) / (HZ * 60), 2),
        }
        for p, (first, last) in PERIODS.items()
    ]
    total = {
        "minutes_tip": 0.0,
        "minutes_otip": 0.0,
        "minutes_played": round(sum(b["minutes_played"] for b in by_period), 2),
        "minutes_played_regular_time": round(sum(b["minutes_played"] for b in by_period), 2),
        "start_frame": 0,
        "end_frame": TOTAL_FRAMES - 1,
    }
    return {
        "player_role": {"id": 17, "position_group": "Other", "name": "Synthetic", "acronym": "SYN"},
        "start_time": "00:00:00" if plays else None,
        "end_time": None,
        "number": player % 10,
        "yellow_card": 0,
        "red_card": 0,
        "injured": False,
        "goal": 0,
        "own_goal": 0,
        "playing_time": {
            "total": total if plays else None,
            "by_period": by_period if plays else [],
        },
        "team_player_id": 5000 + player,
        "team_id": team,
        "id": player,
        "first_name": f"Player{player}",
        "last_name": "Synthetic",
        "short_name": f"P. {player}",
        "birthday": None,
        "trackable_object": 9000 + player,
        "gender": "male",
    }


def _match_json(match_id: int) -> dict[str, Any]:
    def team(team_id: int, name: str, acronym: str) -> dict[str, Any]:
        return {"id": team_id, "name": name, "short_name": name, "acronym": acronym}

    def kit(team_id: int) -> dict[str, Any]:
        season = {"id": 1, "start_year": 2024, "end_year": 2025, "name": "2024/2025"}
        return {
            "id": 100 + team_id,
            "team_id": team_id,
            "season": season,
            "name": "Home",
            "jersey_color": "#ffffff",
            "number_color": "#000000",
        }

    periods = [
        {
            "period": p,
            "name": f"period_{p}",
            "start_frame": first,
            "end_frame": last,
            "duration_frames": last - first,
            "duration_minutes": round((last - first) / (HZ * 60), 2),
        }
        for p, (first, last) in PERIODS.items()
    ]
    roster = [(p, HOME_TEAM) for p in (*HOME_PLAYERS, BENCH_PLAYER)] + [
        (p, AWAY_TEAM) for p in AWAY_PLAYERS
    ]
    return {
        "id": match_id,
        "home_team_score": 1,
        "away_team_score": 0,
        "date_time": "2026-01-01T12:00:00Z",
        "stadium": {"id": 1, "name": "Synthetic Park", "city": "Nowhere", "capacity": None},
        "home_team": team(HOME_TEAM, "Synthetic Home", "SYH"),
        "home_team_kit": kit(HOME_TEAM),
        "away_team": team(AWAY_TEAM, "Synthetic Away", "SYA"),
        "away_team_kit": kit(AWAY_TEAM),
        "home_team_coach": None,
        "away_team_coach": None,
        "home_team_playing_time": {"minutes_tip": 0.0, "minutes_otip": 0.0},
        "away_team_playing_time": {"minutes_tip": 0.0, "minutes_otip": 0.0},
        "competition_edition": {
            "id": 1,
            "competition": {
                "id": 1,
                "area": "XXX",
                "name": "Synthetic League",
                "gender": "male",
                "age_group": "adult",
            },
            "season": {"id": 1, "start_year": 2024, "end_year": 2025, "name": "2024/2025"},
            "name": "XXX - Synthetic League - 2024/2025",
        },
        "match_periods": periods,
        "competition_round": {
            "id": 1,
            "name": "Round 1",
            "round_number": 1,
            "potential_overtime": False,
        },
        "referees": [],
        "players": [_player_json(p, t) for p, t in roster],
        "status": "closed",
        "home_team_side": list(HOME_TEAM_SIDE),
        "ball": {"trackable_object": 1},
        "pitch_length": 105,
        "pitch_width": 68,
    }


def _matches_json(match_id: int) -> list[dict[str, Any]]:
    return [
        {
            "id": match_id,
            "date_time": "2026-01-01T12:00:00Z",
            "home_team": {"id": HOME_TEAM, "short_name": "Synthetic Home"},
            "away_team": {"id": AWAY_TEAM, "short_name": "Synthetic Away"},
            "status": "closed",
            "competition_id": 1,
            "season_id": 1,
            "competition_edition_id": 1,
        }
    ]


# --- the writer ---------------------------------------------------------------------------


def _write(root: Path, rel: str, text: str) -> dict[str, Any]:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    data = text.encode("utf-8")
    path.write_bytes(data)  # bytes, not text mode: LF on every platform
    return {"path": rel, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def write_synthetic_match(
    out_dir: Path,
    *,
    match_id: int = DEFAULT_MATCH_ID,
    seed: int = DEFAULT_SEED,
    runs: Sequence[PlantedRun] = PLANTED_RUNS,
) -> SyntheticMatch:
    """Write one synthetic match under ``out_dir`` and return the reference it was built to.

    Layout: ``out_dir/bundle/data/...`` is a raw bundle in SkillCorner's layout (what
    ``ftbase ingest --from-existing out_dir/bundle`` verifies in place) and
    ``out_dir/manifest.json`` is its manifest, with the sha256 and size of every file, source
    id ``opendata-football`` and a placeholder commit of forty zeros. Pass that manifest to
    ``ftbase ingest|bronze|silver --manifest``. Any existing file is overwritten; the folder
    is created.

    Contract, all of it deterministic (same arguments, same bytes, no network, no clock):

    - two periods, frames 0-59 and 70-129 at 10 Hz (:data:`PERIODS`), frames 60-69 outside
      both; ``home_team_side`` is ``left_to_right`` then ``right_to_left``;
    - three home players (71, 72, 73), one home player who never plays (74), three away
      players (81, 82, 83) and the ball; every player is in every in-period frame;
    - ``is_detected`` is false for ``(player_id, frame)`` exactly when the frame is inside
      one of ``runs``, true for everything else including the whole ball; ``runs`` must lie
      inside one period, name a player on the pitch and neither overlap nor touch another run
      of the same player (``ValueError`` otherwise);
    - six player possessions, four phases of play, all consistent with ``home_team_side``;
    - ``seed`` only moves coordinates, never the structure or the runs.

    Every file is written as UTF-8 with LF line endings. The match is synthetic: nothing
    measured on it may appear in a report.
    """
    _check_runs(runs)
    root = out_dir / "bundle"
    folder = f"data/matches/{match_id}/{match_id}"
    entries = [
        ("matches", None, _write(root, "data/matches.json", json.dumps(_matches_json(match_id)))),
        (
            "match",
            match_id,
            _write(root, f"{folder}_match.json", json.dumps(_match_json(match_id))),
        ),
        (
            "tracking",
            match_id,
            _write(
                root,
                f"{folder}_tracking_extrapolated.jsonl",
                "\n".join(_tracking_lines(runs, seed)) + "\n",
            ),
        ),
        ("events", match_id, _write(root, f"{folder}_dynamic_events.csv", _events_csv(match_id))),
        ("phases", match_id, _write(root, f"{folder}_phases_of_play.csv", _phases_csv(match_id))),
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "sources": [
            {
                "source_id": SOURCE_ID,
                "sport": "football",
                "kind": "github",
                "repo": "synthetic/ftbase-fixtures",
                "ref": "fixtures",
                "commit": FAKE_COMMIT,
                "license": "synthetic: generated by ftbase.fixtures, no provider data",
                "raw_url": "https://synthetic.invalid/{commit}/",
                "lfs_url": "https://synthetic.invalid/{commit}/",
                "files": [
                    {**info, "lfs": False, "role": role, "match_id": mid}
                    for role, mid, info in entries
                ],
            }
        ],
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_bytes((json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
    return SyntheticMatch(
        root=root,
        manifest=manifest_path,
        match_id=match_id,
        runs=tuple(runs),
        home_team_side=HOME_TEAM_SIDE,
        periods=dict(PERIODS),
    )
