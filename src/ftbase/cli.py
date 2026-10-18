"""Command-line entry point."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path

from ftbase import __version__
from ftbase.ingest.fetch import ingest_source, make_client
from ftbase.ingest.local import ingest_local
from ftbase.ingest.manifest import load_manifest
from ftbase.paths import data_root

SOURCE_ID = "opendata-football"


def default_manifest() -> Path:
    """The manifest shipped inside the package (it pins SkillCorner/opendata at one commit)."""
    return Path(str(files("ftbase") / "manifest" / "sources.json"))


def _cmd_ingest(args: argparse.Namespace) -> int:
    source = load_manifest(args.manifest).source(SOURCE_ID)
    raw_root: Path = args.data_dir / "raw" / source.source_id
    try:
        if args.from_existing is not None:
            receipt = ingest_local(source, args.from_existing, raw_root, match_id=args.match)
        else:
            with make_client() as client:
                receipt = ingest_source(client, source, raw_root, match_id=args.match)
    except KeyError as exc:
        sys.exit(f"ftbase: {exc.args[0]}")
    print(receipt.summary)
    return 1 if receipt.failed else 0


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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    func = getattr(args, "func", None)
    return 0 if func is None else int(func(args))


if __name__ == "__main__":
    sys.exit(main())
