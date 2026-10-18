# New in ftbase (no tracklab origin).
"""``ftbase.fixtures`` end to end: ingest, bronze and silver run on the synthetic match.

The reference is the generator's own record of what it planted (``SyntheticMatch.runs``),
never a table the pipeline wrote, so the test cannot pass by comparing silver with itself.
The generator's constants are also pinned here by hand (``HAND_WRITTEN_RUNS``): changing a
planted length in ``fixtures.py`` changes the data and the reference together and the
pipeline test would still agree, so this is the check that notices.

The ingest step is the real one: the generator writes a manifest whose sha256 values are
those of the bytes it wrote, so ``ftbase ingest --from-existing`` verifies them and nothing
has to be skipped.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from ftbase.cli import main
from ftbase.fixtures import PLANTED_RUNS, PlantedRun, SyntheticMatch, write_synthetic_match
from ftbase.silver import read_dim, read_frames
from ftbase.silver.checks import assertion_checks, run_checks
from ftbase.silver.read import connect

# (player, period) -> lengths of the runs of is_detected = false, in frame order.
HAND_WRITTEN_RUNS: dict[tuple[int, int], list[int]] = {
    (71, 1): [4, 7],
    (72, 2): [3],
    (81, 1): [10],
    (81, 2): [6],
    (82, 2): [5],
    (83, 2): [2],
}
TRACKING = "data/matches/900001/900001_tracking_extrapolated.jsonl"


@dataclass(frozen=True)
class Built:
    match: SyntheticMatch
    data_dir: Path


def run_pipeline(
    tmp_path: Path, runs: tuple[PlantedRun, ...] = PLANTED_RUNS, seed: int = 1
) -> Built:
    match = write_synthetic_match(tmp_path / "fx", runs=runs, seed=seed)
    data_dir = tmp_path / "data"
    manifest = ["--manifest", str(match.manifest), "--data-dir", str(data_dir)]
    assert main(["ingest", *manifest, "--from-existing", str(match.root)]) == 0
    assert main(["bronze", *manifest]) == 0
    assert main(["silver", *manifest]) == 0
    return Built(match, data_dir)


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Built:
    return run_pipeline(tmp_path_factory.mktemp("synthetic"))


def planted_lengths(runs: tuple[PlantedRun, ...]) -> dict[tuple[int, int], list[int]]:
    """The reference: what the generator says it planted, as (player, period) -> lengths."""
    out: dict[tuple[int, int], list[int]] = {}
    for run in sorted(runs, key=lambda r: (r.player_id, r.period, r.start_frame)):
        out.setdefault((run.player_id, run.period), []).append(run.length)
    return out


def recovered_lengths(built: Built) -> dict[tuple[int, int], list[int]]:
    """What silver says: runs of consecutive frames with is_detected false, per player/period."""
    rows = read_frames(
        [built.match.match_id],
        columns=["entity_id", "entity_kind", "period", "frame_native", "is_detected"],
        data_dir=built.data_dir,
    ).to_pylist()
    players = sorted(
        (r for r in rows if r["entity_kind"] == "player"),
        key=lambda r: (r["entity_id"], r["period"], r["frame_native"]),
    )
    out: dict[tuple[int, int], list[int]] = {}
    for (entity, period), group in itertools.groupby(
        players, key=lambda r: (r["entity_id"], r["period"])
    ):
        frames = [r["frame_native"] for r in group if not r["is_detected"]]
        # a run is a stretch of consecutive frame numbers
        for _, run in itertools.groupby(enumerate(frames), key=lambda t: t[1] - t[0]):
            out.setdefault((int(entity.removeprefix("fb:")), period), []).append(len(list(run)))
    return out


# --- the generator -------------------------------------------------------------------------


def test_the_planted_lengths_are_the_ones_written_down_here() -> None:
    assert planted_lengths(PLANTED_RUNS) == HAND_WRITTEN_RUNS


def test_same_seed_same_bytes_and_a_new_seed_moves_only_coordinates(tmp_path: Path) -> None:
    a = write_synthetic_match(tmp_path / "a", seed=5)
    b = write_synthetic_match(tmp_path / "b", seed=5)
    c = write_synthetic_match(tmp_path / "c", seed=6)
    files = sorted(p.relative_to(a.root) for p in a.root.rglob("*") if p.is_file())
    assert len(files) == 5
    for rel in files:
        assert (a.root / rel).read_bytes() == (b.root / rel).read_bytes(), rel
        assert b"\r" not in (a.root / rel).read_bytes(), f"{rel} is not LF"
    assert a.manifest.read_bytes() == b.manifest.read_bytes()
    assert (a.root / TRACKING).read_bytes() != (c.root / TRACKING).read_bytes()

    def flags(root: Path) -> list[list[bool]]:
        lines = (root / TRACKING).read_text(encoding="utf-8").splitlines()
        return [[p["is_detected"] for p in json.loads(x)["player_data"]] for x in lines]

    assert flags(a.root) == flags(c.root), "the seed never touches the planted runs"


def test_the_manifest_holds_the_hash_and_size_of_every_file(tmp_path: Path) -> None:
    match = write_synthetic_match(tmp_path)
    files = json.loads(match.manifest.read_text(encoding="utf-8"))["sources"][0]["files"]
    assert sorted(f["role"] for f in files) == ["events", "match", "matches", "phases", "tracking"]
    assert all((match.root / f["path"]).stat().st_size == f["bytes"] for f in files)


@pytest.mark.parametrize(
    "runs",
    [
        (PlantedRun(71, 1, 5, 4), PlantedRun(71, 1, 9, 2)),  # touching: would read back as one
        (PlantedRun(71, 1, 5, 4), PlantedRun(71, 1, 7, 4)),  # overlapping
        (PlantedRun(71, 1, 55, 6),),  # leaves period 1 (ends at frame 59)
        (PlantedRun(71, 3, 5, 2),),  # no such period
        (PlantedRun(74, 1, 5, 2),),  # the bench player is not on the pitch
        (PlantedRun(71, 1, 5, 0),),  # empty
    ],
)
def test_runs_that_cannot_be_read_back_are_refused(
    tmp_path: Path, runs: tuple[PlantedRun, ...]
) -> None:
    with pytest.raises(ValueError, match="PlantedRun"):
        write_synthetic_match(tmp_path, runs=runs)


def test_the_cli_writes_the_match(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["fixtures", "--out", str(tmp_path)]) == 0
    assert "7 planted runs" in capsys.readouterr().out
    assert (tmp_path / "manifest.json").is_file()


# --- the pipeline on it --------------------------------------------------------------------


def test_silver_has_the_frames_and_players_the_match_was_built_with(built: Built) -> None:
    rows = read_frames([built.match.match_id], data_dir=built.data_dir).to_pylist()
    in_period = sum(last - first + 1 for first, last in built.match.periods.values())
    assert in_period == 120
    assert len(rows) == in_period * 7, "six players and the ball in every in-period frame"
    assert {r["frame_native"] for r in rows} == {
        f for first, last in built.match.periods.values() for f in range(first, last + 1)
    }, "frames 60-69 lie in no period and are not in silver"
    players = {r["entity_id"] for r in rows if r["entity_kind"] == "player"}
    assert players == {f"fb:{p}" for p in (71, 72, 73, 81, 82, 83)}
    entities = read_dim("dim_entity", data_dir=built.data_dir).to_pylist()
    assert len(entities) == 8, "the bench player and the ball are in the dimension, not in frames"


def test_the_runs_silver_recovers_are_the_planted_ones(built: Built) -> None:
    assert recovered_lengths(built) == planted_lengths(built.match.runs)
    rows = read_frames([built.match.match_id], data_dir=built.data_dir).to_pylist()
    undetected = {(r["entity_id"], r["frame_native"]) for r in rows if not r["is_detected"]}
    planted = {(f"fb:{r.player_id}", f) for r in built.match.runs for f in r.frames}
    assert undetected == planted, "no other frame, and no ball frame, is extrapolated"


def test_other_planted_runs_are_recovered_too(tmp_path: Path) -> None:
    runs = (PlantedRun(73, 1, 0, 1), PlantedRun(73, 1, 2, 2), PlantedRun(83, 2, 100, 30))
    other = run_pipeline(tmp_path, runs=runs, seed=9)
    assert planted_lengths(runs) == {(73, 1): [1, 2], (83, 2): [30]}
    assert recovered_lengths(other) == planted_lengths(runs)


def test_the_teams_attack_in_opposite_directions_and_swap_at_half_time(built: Built) -> None:
    rows = read_dim("dim_team_period", data_dir=built.data_dir).to_pylist()
    signs = {(r["period"], r["team_id"]): r["attack_sign"] for r in rows}
    assert signs == {(1, "fb:7"): 1, (1, "fb:8"): -1, (2, "fb:7"): -1, (2, "fb:8"): 1}
    attacking = read_frames(
        [built.match.match_id],
        attacking=True,
        columns=["period", "team_in_possession_id", "attack_sign"],
        data_dir=built.data_dir,
    ).to_pylist()
    assert attacking and all(
        r["attack_sign"] == signs[(r["period"], r["team_in_possession_id"])] for r in attacking
    )


def test_every_check_and_the_six_assertions_come_back_empty(built: Built) -> None:
    with connect(built.data_dir) as con:
        assertions = run_checks(con, assertion_checks())
        everything = run_checks(con)
    assert len(assertions) == 6
    assert [(r.name, r.n_rows) for r in assertions if r.n_rows] == []
    assert [(r.name, r.n_rows) for r in everything if r.n_rows] == []


def test_a_changed_byte_in_the_bundle_fails_ingest(tmp_path: Path) -> None:
    match = write_synthetic_match(tmp_path / "fx")
    track = match.root / TRACKING
    track.write_bytes(track.read_bytes().replace(b'"frame": 5,', b'"frame": 6,', 1))
    args = ["ingest", "--manifest", str(match.manifest), "--data-dir", str(tmp_path / "d")]
    assert main([*args, "--from-existing", str(match.root)]) == 1
