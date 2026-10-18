# Rewritten from tracklab@686508a: pipeline/src/tracklab/cli.py
# Changes: football only, down to the ingest, bronze and silver subcommands; `fixtures` and
# `provenance` are new.
"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Sequence
from importlib.resources import files
from pathlib import Path

from ftbase import __version__
from ftbase.bronze.common import raw_root_for
from ftbase.bronze.football.events import build_events
from ftbase.bronze.football.meta import build_meta
from ftbase.bronze.football.tracking import build_tracking
from ftbase.fixtures import DEFAULT_SEED, write_synthetic_match
from ftbase.ingest.fetch import RECEIPT_NAME, ingest_source, make_client
from ftbase.ingest.local import ingest_local
from ftbase.ingest.manifest import Source, load_manifest
from ftbase.ingest.select import select_match
from ftbase.paths import data_root
from ftbase.provenance import ProvenanceError, check, render_summary, write
from ftbase.silver.build import build_silver

SOURCE_ID = "opendata-football"

# Table group -> builder(source, raw_root, data_dir). Registered in the order they run.
BRONZE_BUILDERS: dict[str, Callable[[Source, Path, Path], object]] = {
    "tracking": build_tracking,
    "meta": build_meta,
    "events": build_events,
}


def default_manifest() -> Path:
    """The manifest shipped inside the package (it pins SkillCorner/opendata at one commit)."""
    return Path(str(files("ftbase") / "manifest" / "sources.json"))


def _cmd_ingest(args: argparse.Namespace) -> int:
    source = load_manifest(args.manifest).source(SOURCE_ID)
    raw_root: Path = args.data_dir / "raw" / source.source_id
    try:
        if args.match is not None:
            chosen = select_match(source, args.match).files
            total = sum(f.bytes for f in chosen)
            print(f"match {args.match}: {len(chosen)} files, {total} bytes")
        if args.from_existing is not None:
            receipt = ingest_local(source, args.from_existing, raw_root, match_id=args.match)
        else:
            with make_client() as client:
                receipt = ingest_source(client, source, raw_root, match_id=args.match)
    except KeyError as exc:
        sys.exit(f"ftbase: {exc.args[0]}")
    print(receipt.summary)
    return 1 if receipt.failed else 0


def receipt_match_id(data_dir: Path) -> int | None:
    """The match a partial ``ingest --match`` verified, from its receipt.

    None for a full ingest, and for a data dir with no receipt (bronze built some other way).
    """
    path = data_dir / "raw" / SOURCE_ID / RECEIPT_NAME
    if not path.is_file():
        return None
    receipt = json.loads(path.read_text(encoding="utf-8"))
    value = receipt.get("match_id")
    return None if value is None else int(value)


def _cmd_bronze(args: argparse.Namespace) -> int:
    groups = [g for g in args.tables.split(",") if g] if args.tables else list(BRONZE_BUILDERS)
    unknown = [g for g in groups if g not in BRONZE_BUILDERS]
    if unknown:
        sys.exit(f"ftbase: no bronze builder for {unknown} (known: {sorted(BRONZE_BUILDERS)})")
    try:
        source = load_manifest(args.manifest).source(SOURCE_ID)
        raw_root = raw_root_for(args.data_dir, SOURCE_ID)
        match_id = receipt_match_id(args.data_dir)
        if match_id is not None:
            source = select_match(source, match_id)
    except (KeyError, FileNotFoundError, RuntimeError) as exc:
        sys.exit(f"ftbase: {exc.args[0]}")
    started = time.perf_counter()
    for group in groups:
        print(f"bronze {group}: {raw_root}")
        BRONZE_BUILDERS[group](source, raw_root, args.data_dir)
    print(f"total seconds={time.perf_counter() - started:.1f}")
    return 0


def _cmd_silver(args: argparse.Namespace) -> int:
    try:
        result = build_silver(
            args.data_dir, args.manifest, match_id=receipt_match_id(args.data_dir)
        )
    except (KeyError, FileNotFoundError, RuntimeError, ValueError) as exc:
        sys.exit(f"ftbase: {exc.args[0]}")
    passed = sum(c.ok for c in result.checks)
    print(f"checks: {passed}/{len(result.checks)} passed")
    for failure in result.failures:
        print(f"FAIL {failure}")
    print(f"total seconds={result.seconds:.1f}")
    return 1 if result.failures else 0


def _cmd_fixtures(args: argparse.Namespace) -> int:
    match = write_synthetic_match(args.out, seed=args.seed)
    print(f"synthetic match {match.match_id}: {len(match.runs)} planted runs")
    print(f"bundle:   {match.root}")
    print(f"manifest: {match.manifest}")
    print(
        f"next: ftbase ingest --manifest {match.manifest} --from-existing {match.root} "
        f"--data-dir <dir>, then bronze and silver with the same --manifest and --data-dir"
    )
    return 0


def _cmd_provenance(args: argparse.Namespace) -> int:
    if args.check:
        problems = check(args.root, args.tracklab)
        for problem in problems:
            print(f"FAIL {problem}")
        if not problems:
            print("provenance: the table, the JSON and the files agree")
        return 1 if problems else 0
    try:
        report = write(args.root, args.tracklab)
    except ProvenanceError as exc:
        sys.exit(f"ftbase: {exc}")
    print(render_summary(report))
    print("wrote reports/provenance.json and the count columns of docs/provenance.md")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ftbase",
        description="Open pipeline for SkillCorner's open football data. "
        "--data-dir defaults to $FTBASE_DATA_ROOT or data.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    ingest = subparsers.add_parser(
        "ingest", help="Download the opendata-football files and verify them against the manifest."
    )
    ingest.add_argument(
        "--manifest",
        type=Path,
        default=default_manifest(),
        help="Path to sources.json (default: the manifest shipped with ftbase).",
    )
    ingest.add_argument(
        "--data-dir",
        type=Path,
        default=data_root(),
        help="Data root; files land under <data-dir>/raw/opendata-football/ (default: data).",
    )
    ingest.add_argument(
        "--from-existing",
        type=Path,
        default=None,
        help="Verify the files in place inside an existing clone (read-only), instead of "
        "downloading.",
    )
    ingest.add_argument(
        "--match",
        type=int,
        default=None,
        metavar="ID",
        help="Only this match (its four files plus matches.json); the run reports PARTIAL.",
    )
    ingest.set_defaults(func=_cmd_ingest)

    bronze = subparsers.add_parser(
        "bronze", help="Convert the verified raw files to typed Parquet (one file per match)."
    )
    bronze.add_argument(
        "--tables",
        default=None,
        help="Comma-separated table groups to build: tracking, meta, events (default: all).",
    )
    bronze.add_argument(
        "--manifest",
        type=Path,
        default=default_manifest(),
        help="Path to sources.json (default: the manifest shipped with ftbase).",
    )
    bronze.add_argument(
        "--data-dir",
        type=Path,
        default=data_root(),
        help="Data root: reads raw/opendata-football/, writes bronze/football/<table>/ "
        "(default: data).",
    )
    bronze.set_defaults(func=_cmd_bronze)

    silver = subparsers.add_parser(
        "silver",
        help="Build silver from bronze with DuckDB (orientation, dims, frames, events, views) "
        "and run its checks; exit 1 if any fails.",
    )
    silver.add_argument(
        "--manifest",
        type=Path,
        default=default_manifest(),
        help="Path to sources.json; dim_match takes the pinned commit from it "
        "(default: the manifest shipped with ftbase).",
    )
    silver.add_argument(
        "--data-dir",
        type=Path,
        default=data_root(),
        help="Data root: reads bronze/football/, writes silver/ (default: data).",
    )
    silver.set_defaults(func=_cmd_silver)

    fixtures = subparsers.add_parser(
        "fixtures",
        help="Write a synthetic match in SkillCorner's raw format, with its manifest "
        "(no network, no provider data; nothing measured on it is a result).",
    )
    fixtures.add_argument("--out", type=Path, required=True, help="Folder to write into.")
    fixtures.add_argument(
        "--seed", type=int, default=DEFAULT_SEED, help="Seed of the coordinates (default: fixed)."
    )
    fixtures.set_defaults(func=_cmd_fixtures)

    provenance = subparsers.add_parser(
        "provenance",
        help="Count the lines copied from tracklab and write reports/provenance.json and the "
        "table's count columns; with --check, verify them against the files (exit 1 on a "
        "difference).",
    )
    provenance.add_argument(
        "--check",
        action="store_true",
        help="Verify the table and the JSON against the files instead of writing them.",
    )
    provenance.add_argument(
        "--tracklab",
        type=Path,
        default=None,
        help="tracklab clone to read the originals from with git show (read only). Optional: "
        "without it a new file needs none and any other file is recounted against this "
        "repository's import commit; a stored count is reused only where that commit does not "
        "hold the origins (see docs/provenance.md). With it every file is recounted there.",
    )
    provenance.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repository root (default: the current folder).",
    )
    provenance.set_defaults(func=_cmd_provenance)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    func = getattr(args, "func", None)
    return 0 if func is None else int(func(args))


if __name__ == "__main__":
    sys.exit(main())
