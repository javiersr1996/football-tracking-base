# Copied from tracklab@686508a: pipeline/src/tracklab/bronze/football/tracking.py
# Changes: imports; Parquet metadata keys ftbase.*; error for a missing pinned commit.
"""Bronze of the football tracking: ``tracking_frames`` and ``tracking_objects``.

Reads ``<match_id>_tracking_extrapolated.jsonl`` line by line (never whole in memory,
each file is 90-102 MB) in batches of 100k frames and writes one Parquet per match and
table with the schemas declared below. Bronze does not correct, filter or interpolate:
every line becomes a row of ``tracking_frames``, every ``player_data`` entry a row of
``tracking_objects``, and values are stored as delivered. Every correction belongs to
silver. See ``docs/formats/football.md`` (task 4) for the measurements this module
relies on instead of re-deriving.

What is different from basketball's ``tracking.py`` (``bronze/basketball/tracking.py``),
whose layout and key-imposition idiom this module copies:

- The file is plain JSONL, not gzipped: ``iter_frames`` opens it as text, no ``gzip``.
- ``is_detected`` is delivered as a native JSON boolean in this bundle (``true``/
  ``false``), for players and for the ball: no 0/1 cast is needed, unlike basketball's
  ``isDetected``. Stored as-is.
- ``ball_data`` always has all four keys (``is_detected, x, y, z``); the four are
  either all non-null (the ball is present) or all null together (measured: 0 frames
  with a partial ball in the 20 matches). Bronze keeps that grouping as four nullable
  columns rather than a presence flag, because the flag would be a derived fact.
- ``image_corners_projection`` is null independently of ``period``: most of its
  null frames are inside a period, not just the frames outside every period. Bronze
  stores the 8 corners as nullable DOUBLE and imposes no extra rule on them.
- Out-of-period frames (``period`` and ``timestamp`` null, ``player_data`` empty,
  ``ball_data`` all null) are a row of ``tracking_frames`` with ``n_players = 0`` and
  no row in ``tracking_objects``: the full timeline the provider delivered, pre-match
  and half-time footage included in the 5 matches that carry it, lives in bronze.
  ``frame`` is contiguous from 0 in the 20 matches (measured by task 4); a frame the
  file never delivered stays absent, never filled with ``range()``.
- ``source_row`` is the JSONL line index and ``frame`` is the provider's own counter;
  task 4 measured them equal in the matches checked so far, but bronze keeps both
  columns rather than assuming line N is frame N, the same discipline as basketball's
  ``frameIdx`` vs ``source_row``.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from ftbase.bronze.common import LINEAGE_FIELDS, ParquetSink, batch_from_columns, bronze_path
from ftbase.ingest.manifest import FileEntry, Source

SPORT = "football"
BATCH_FRAMES = 100_000
Log = Callable[[str], None]

# Exactly the keys the provider delivers, frozen by task 4 over the 20 matches. One
# more or one less fails the run: see ``docs/formats/football.md``, "frame_keys".
FRAME_KEYS = frozenset(
    {
        "ball_data",
        "frame",
        "image_corners_projection",
        "period",
        "player_data",
        "possession",
        "timestamp",
    }
)
PLAYER_KEYS = frozenset({"is_detected", "player_id", "x", "y"})
BALL_KEYS = frozenset({"is_detected", "x", "y", "z"})
POSSESSION_KEYS = frozenset({"group", "player_id"})
CORNER_KEYS = frozenset(
    {
        "x_bottom_left",
        "x_bottom_right",
        "x_top_left",
        "x_top_right",
        "y_bottom_left",
        "y_bottom_right",
        "y_top_left",
        "y_top_right",
    }
)
CORNER_ORDER = tuple(sorted(CORNER_KEYS))

TRACKING_FRAMES = pa.schema(
    [
        *LINEAGE_FIELDS,
        pa.field("frame", pa.int64(), nullable=False),
        pa.field("timestamp", pa.string()),  # null iff outside every period (task 4)
        pa.field("period", pa.int16()),
        pa.field("ball_x", pa.float64()),
        pa.field("ball_y", pa.float64()),
        pa.field("ball_z", pa.float64()),
        pa.field("ball_is_detected", pa.bool_()),
        pa.field("possession_group", pa.string()),  # 'home team' | 'away team' | null
        pa.field("possession_player_id", pa.int64()),
        pa.field("n_players", pa.int16(), nullable=False),  # len(player_data): 0 or 22 in-period
        *(pa.field(name, pa.float64()) for name in CORNER_ORDER),
    ]
)

TRACKING_OBJECTS = pa.schema(
    [
        *LINEAGE_FIELDS,
        pa.field("frame", pa.int64(), nullable=False),
        pa.field("player_id", pa.int64(), nullable=False),
        pa.field("x", pa.float64(), nullable=False),
        pa.field("y", pa.float64(), nullable=False),
        pa.field("is_detected", pa.bool_(), nullable=False),  # never null (task 4, 0/20 matches)
    ]
)

TABLES = {"tracking_frames": TRACKING_FRAMES, "tracking_objects": TRACKING_OBJECTS}


@dataclass(frozen=True)
class TrackingStats:
    match_id: int
    frames: int
    objects: int
    seconds: float
    mb: float


def iter_frames(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """(line number, frame) for every line of the JSONL, one line at a time."""
    with path.open("r", encoding="utf-8") as fh:
        for row, line in enumerate(fh):
            yield row, json.loads(line)


def frame_batches(
    path: Path, match_id: int, source_file: str, batch_frames: int = BATCH_FRAMES
) -> Iterator[tuple[pa.RecordBatch, pa.RecordBatch]]:
    """Pairs of (tracking_frames batch, tracking_objects batch) per ``batch_frames`` lines."""
    frames: dict[str, list[Any]] = {name: [] for name in TRACKING_FRAMES.names[4:]}
    objects: dict[str, list[Any]] = {name: [] for name in TRACKING_OBJECTS.names[4:]}
    f_row: list[int] = []
    o_row: list[int] = []
    lineage = {"sport": SPORT, "match_id": match_id, "source_file": source_file}

    def flush() -> tuple[pa.RecordBatch, pa.RecordBatch]:
        fb = batch_from_columns(
            TRACKING_FRAMES, {**lineage, "source_row": f_row, **frames}, len(f_row)
        )
        ob = batch_from_columns(
            TRACKING_OBJECTS, {**lineage, "source_row": o_row, **objects}, len(o_row)
        )
        f_row.clear()
        o_row.clear()
        for column in (*frames.values(), *objects.values()):
            column.clear()
        return fb, ob

    for row, frame in iter_frames(path):
        try:
            if frame.keys() != FRAME_KEYS:
                raise ValueError(f"frame keys {sorted(frame)} != {sorted(FRAME_KEYS)}")
            fr: int = frame["frame"]
            ball = frame["ball_data"]
            if ball.keys() != BALL_KEYS:
                raise ValueError(f"ball_data keys {sorted(ball)} != {sorted(BALL_KEYS)}")
            possession = frame["possession"]
            if possession.keys() != POSSESSION_KEYS:
                raise ValueError(
                    f"possession keys {sorted(possession)} != {sorted(POSSESSION_KEYS)}"
                )
            corners = frame["image_corners_projection"]
            if corners.keys() != CORNER_KEYS:
                raise ValueError(
                    f"image_corners_projection keys {sorted(corners)} != {sorted(CORNER_KEYS)}"
                )
            players = frame["player_data"]

            f_row.append(row)
            frames["frame"].append(fr)
            frames["timestamp"].append(frame["timestamp"])
            frames["period"].append(frame["period"])
            frames["ball_x"].append(ball["x"])
            frames["ball_y"].append(ball["y"])
            frames["ball_z"].append(ball["z"])
            frames["ball_is_detected"].append(ball["is_detected"])
            frames["possession_group"].append(possession["group"])
            frames["possession_player_id"].append(possession["player_id"])
            frames["n_players"].append(len(players))
            for name in CORNER_ORDER:
                frames[name].append(corners[name])

            for player in players:
                if player.keys() != PLAYER_KEYS:
                    raise ValueError(f"player keys {sorted(player)} != {sorted(PLAYER_KEYS)}")
                o_row.append(row)
                objects["frame"].append(fr)
                objects["player_id"].append(player["player_id"])
                objects["x"].append(player["x"])
                objects["y"].append(player["y"])
                objects["is_detected"].append(player["is_detected"])
        except (KeyError, ValueError, TypeError, AttributeError) as exc:
            raise ValueError(f"{source_file} line {row}: {exc}") from exc
        if len(f_row) >= batch_frames:
            yield flush()
    if f_row:
        yield flush()


def build_tracking_match(
    entry: FileEntry, raw_root: Path, data_dir: Path, commit: str
) -> TrackingStats:
    """Write ``tracking_frames/<id>.parquet`` and ``tracking_objects/<id>.parquet`` of one match."""
    assert entry.match_id is not None
    started = time.perf_counter()
    metadata = {
        "ftbase.sport": SPORT,
        "ftbase.source_file": entry.path,
        "ftbase.source_sha256": entry.sha256,
        "ftbase.source_commit": commit,
    }
    frames_path = bronze_path(data_dir, SPORT, "tracking_frames", entry.match_id)
    objects_path = bronze_path(data_dir, SPORT, "tracking_objects", entry.match_id)
    with (
        ParquetSink(frames_path, TRACKING_FRAMES, metadata) as frames,
        ParquetSink(objects_path, TRACKING_OBJECTS, metadata) as objects,
    ):
        for frames_batch, objects_batch in frame_batches(
            raw_root / entry.path, entry.match_id, entry.path
        ):
            frames.write(frames_batch)
            objects.write(objects_batch)
    return TrackingStats(
        match_id=entry.match_id,
        frames=frames.rows,
        objects=objects.rows,
        seconds=time.perf_counter() - started,
        mb=(frames.bytes + objects.bytes) / 1e6,
    )


def build_tracking(
    source: Source, raw_root: Path, data_dir: Path, log: Log = print
) -> list[TrackingStats]:
    """Every tracking file the manifest pins, in match id order, one line printed per match."""
    if source.commit is None:
        raise RuntimeError(f"{source.source_id} has no pinned commit in the manifest")
    entries = sorted(
        (f for f in source.files if f.role == "tracking" and f.match_id is not None),
        key=lambda f: f.match_id or 0,
    )
    log(f"{'match_id':>8} {'frames':>8} {'objects':>9} {'seconds':>8} {'MB':>7}")
    stats: list[TrackingStats] = []
    for entry in entries:
        s = build_tracking_match(entry, raw_root, data_dir, source.commit)
        log(f"{s.match_id:>8} {s.frames:>8} {s.objects:>9} {s.seconds:>8.1f} {s.mb:>7.1f}")
        stats.append(s)
    return stats
