# New in ftbase (no tracklab origin): `ftbase provenance`, the line counts of this table.
"""Provenance of the files ftbase took from tracklab, counted by script.

Scope: every file under ``src/ftbase/`` and ``tests/`` (``__pycache__`` and ``*.pyc`` aside).
``docs/provenance.md`` has one row per file: its origin in tracklab (paths and commit), its
state (``copiado``, ``reescrito`` or ``nuevo``) and two count columns. The same counts are in
``reports/provenance.json``.

A line is a newline-terminated line, the unit ``wc -l`` counts (CRLF is read as LF). Lines
copied are the lines of the current file that ``difflib`` matches, in order, to the original:
the origin paths of the row read with ``git show <commit>:<path>`` in tracklab, concatenated in
the order the table lists them. Lines changed are the rest. A ``nuevo`` file has no origin, so
none of its lines is copied.

Where the originals come from: ``--tracklab <clone>`` reads them from tracklab; without it, a
``nuevo`` row needs none, a file whose sha256 is the one in ``reports/provenance.json`` keeps
its stored count, and an edited ``copiado`` or ``reescrito`` file is recounted against the
original as it sits in this repository's own import commit (see :func:`import_commit`).
The recount is always made when the import commit holds the origins; the stored count is only
reused for a file whose origins it does not hold, and only if the file's sha256, origin paths,
commit and state all equal the ones stored with it.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final

SCOPE_DIRS: Final = ("src/ftbase", "tests")
STATES: Final = ("copiado", "reescrito", "nuevo")
DOC: Final = "docs/provenance.md"
REPORT: Final = "reports/provenance.json"

_ROW: Final = re.compile(r"^\| `([^`]+)` \|")
_TICK: Final = re.compile(r"`([^`]+)`")


class ProvenanceError(Exception):
    """A count that cannot be made, or a table that does not describe the scope."""


@dataclass(frozen=True)
class Row:
    path: str
    origins: tuple[str, ...]
    commit: str
    state: str


@dataclass(frozen=True)
class Entry:
    row: Row
    sha256: str
    lines_total: int
    lines_copied: int

    @property
    def lines_changed(self) -> int:
        return self.lines_total - self.lines_copied


@dataclass(frozen=True)
class Report:
    entries: tuple[Entry, ...]

    def summary(self) -> dict[str, dict[str, int]]:
        summary: dict[str, dict[str, int]] = {}
        for state in STATES:
            rows = [e for e in self.entries if e.row.state == state]
            summary[state] = {
                "files": len(rows),
                "lines_total": sum(e.lines_total for e in rows),
                "lines_copied": sum(e.lines_copied for e in rows),
                "lines_changed": sum(e.lines_changed for e in rows),
            }
        return summary


def split_lines(data: bytes) -> list[str]:
    """Lines with their LF (CRLF read as LF); a text that ends in LF has ``wc -l`` lines."""
    pieces = data.decode("utf-8").replace("\r\n", "\n").split("\n")
    lines = [piece + "\n" for piece in pieces[:-1]]
    if pieces[-1]:
        lines.append(pieces[-1])
    return lines


def count_copied(original: list[str], current: list[str]) -> int:
    """Lines of ``current`` matched in order to ``original`` (no line is treated as junk)."""
    matcher = difflib.SequenceMatcher(None, original, current, autojunk=False)
    return sum(block.size for block in matcher.get_matching_blocks())


def scope_files(root: Path) -> list[str]:
    found: list[str] = []
    for directory in SCOPE_DIRS:
        for path in (root / directory).rglob("*"):
            rel = path.relative_to(root)
            if path.is_file() and "__pycache__" not in rel.parts and path.suffix != ".pyc":
                found.append(rel.as_posix())
    return sorted(found)


def read_rows(doc: str) -> list[Row]:
    rows: list[Row] = []
    for line in doc.split("\n"):
        if _ROW.match(line) is None:
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        commit = cells[2].strip("`")
        rows.append(
            Row(
                path=cells[0].strip("`"),
                origins=tuple(_TICK.findall(cells[1])),
                commit="" if commit == "-" else commit,
                state=cells[3],
            )
        )
    return rows


def _git_show(repo: Path, commit: str, path: str) -> bytes:
    # Read only: `git show` does not touch the clone.
    result = subprocess.run(
        ["git", "-C", str(repo), "show", f"{commit}:{path}"],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise ProvenanceError(f"git show {commit}:{path} in {repo} failed: {detail}")
    return result.stdout


def import_message(tracklab_commit: str) -> str:
    return f"Import football base from tracklab@{tracklab_commit}"


def import_commit(root: Path, tracklab_commit: str) -> str | None:
    """The commit of this repository that imported tracklab ``tracklab_commit``, or None.

    Found by its message, never by a fixed sha, because the history may be rewritten. None when
    ``root`` is not a git repository, the clone is shallow, or no such commit is reachable.
    """
    result = subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "log",
            "--fixed-strings",
            f"--grep={import_message(tracklab_commit)}",
            "--format=%H %s",
        ],
        capture_output=True,
        check=False,
    )
    prefix = import_message(tracklab_commit)
    lines = [ln for ln in result.stdout.decode("utf-8", "replace").splitlines() if ln]
    hashes = [ln.split(" ", 1)[0] for ln in lines if ln.split(" ", 1)[-1].startswith(prefix)]
    return hashes[-1] if result.returncode == 0 and hashes else None


def _read(path: Path) -> str:
    return path.read_bytes().decode("utf-8") if path.is_file() else ""


@dataclass(frozen=True)
class Stored:
    """What ``reports/provenance.json`` recorded for one file."""

    sha256: str
    origins: tuple[str, ...]
    commit: str
    state: str
    lines_copied: int

    def describes(self, row: Row, digest: str) -> bool:
        return (self.sha256, self.origins, self.commit, self.state) == (
            digest,
            row.origins,
            row.commit,
            row.state,
        )


def _load_stored(root: Path) -> dict[str, Stored]:
    """``path -> Stored`` from the JSON on disk; empty if there is none."""
    path = root / REPORT
    if not path.is_file():
        return {}
    try:
        files = json.loads(_read(path))["files"]
        return {
            f["path"]: Stored(
                str(f["sha256"]),
                tuple(str(o) for o in f["origins"]),
                str(f["commit"]),
                str(f["state"]),
                int(f["lines_copied"]),
            )
            for f in files
        }
    except (ValueError, KeyError, TypeError):
        return {}


def _original(repo: Path, commit: str, origins: tuple[str, ...]) -> list[str]:
    original: list[str] = []
    for origin in origins:
        original += split_lines(_git_show(repo, commit, origin))
    return original


def _recount_offline(root: Path, row: Row, current: list[str], kept: Stored | None) -> int:
    """Copied lines of a file against the original in this repository's import commit.

    When that commit does not hold the origins, the stored count is kept if it still describes
    this exact file and row; otherwise there is nothing to count against.
    """
    imported = import_commit(root, row.commit)
    if imported is None:
        if kept is not None:
            return kept.lines_copied
        raise ProvenanceError(
            f"{row.path}: edited since {REPORT} was written and the original cannot be read: "
            f"no commit '{import_message(row.commit)}' is reachable from {root} (a shallow "
            f"clone?). Fetch the full history or pass --tracklab <clone of tracklab>"
        )
    try:
        original = _original(root, imported, row.origins)
    except ProvenanceError as exc:
        if kept is not None:
            return kept.lines_copied
        raise ProvenanceError(
            f"{row.path}: edited since {REPORT} was written, and its origin "
            f"{', '.join(row.origins)} is not in the import commit {imported[:7]}: "
            f"pass --tracklab <clone of tracklab>"
        ) from exc
    return count_copied(original, current)


def build(root: Path, tracklab: Path | None, stored: Mapping[str, Stored] | None) -> Report:
    """Count every row. Copied lines come from tracklab when given.

    Without it: a ``nuevo`` row has none; any other file is recounted against the original in
    this repository's import commit, and keeps its stored count only if that commit does not hold
    the origins and the stored entry matches the file's sha256 and its row (see :class:`Stored`).
    """
    rows = read_rows(_read(root / DOC))
    scoped = set(scope_files(root))
    in_table = {row.path for row in rows}
    problems = [f"{p}: no row in {DOC}" for p in sorted(scoped - in_table)]
    problems += [f"{r.path}: row without a file in scope" for r in rows if r.path not in scoped]
    entries: list[Entry] = []
    for row in rows:
        if row.path not in scoped:
            continue
        if row.state not in STATES:
            problems.append(f"{row.path}: unknown state {row.state!r}")
            continue
        if (row.state == "nuevo") != (not row.origins and not row.commit):
            problems.append(f"{row.path}: origin columns do not match state {row.state}")
            continue
        data = (root / row.path).read_bytes()
        if data and not data.endswith(b"\n"):
            problems.append(f"{row.path}: does not end with LF")
        current = split_lines(data)
        digest = hashlib.sha256(data).hexdigest()
        copied = 0
        if row.state != "nuevo":
            try:
                if tracklab is not None:
                    copied = count_copied(_original(tracklab, row.commit, row.origins), current)
                else:
                    entry_kept = (stored or {}).get(row.path)
                    kept = entry_kept if entry_kept and entry_kept.describes(row, digest) else None
                    copied = _recount_offline(root, row, current, kept)
            except ProvenanceError as exc:
                problems.append(str(exc))
                continue
        if not 0 <= copied <= len(current):
            problems.append(f"{row.path}: copied count {copied} outside 0..{len(current)}")
            copied = min(max(copied, 0), len(current))
        entries.append(Entry(row, digest, len(current), copied))
    if problems:
        raise ProvenanceError("\n".join(problems))
    return Report(tuple(sorted(entries, key=lambda e: e.row.path)))


def render_json(report: Report) -> str:
    data = {
        "files": [
            {
                "path": e.row.path,
                "origins": list(e.row.origins),
                "commit": e.row.commit,
                "state": e.row.state,
                "sha256": e.sha256,
                "lines_total": e.lines_total,
                "lines_copied": e.lines_copied,
                "lines_changed": e.lines_changed,
            }
            for e in report.entries
        ],
        "summary": report.summary(),
    }
    return json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def fill_table(doc: str, report: Report) -> str:
    """The doc with the two count cells of every file row replaced; nothing else changes."""
    by_path = {e.row.path: e for e in report.entries}
    out: list[str] = []
    for line in doc.split("\n"):
        match = _ROW.match(line)
        if match is not None and match.group(1) in by_path:
            entry = by_path[match.group(1)]
            cells = line.split("|")
            cells[-3] = f" {entry.lines_copied} "
            cells[-2] = f" {entry.lines_changed} "
            line = "|".join(cells)
        out.append(line)
    return "\n".join(out)


def render_summary(report: Report) -> str:
    header = f"{'state':<10}{'files':>6}{'total':>8}{'copied':>8}{'changed':>9}"
    lines = [header]
    summary = report.summary()
    for state, figures in summary.items():
        lines.append(
            f"{state:<10}{figures['files']:>6}{figures['lines_total']:>8}"
            f"{figures['lines_copied']:>8}{figures['lines_changed']:>9}"
        )
    keys = ("files", "lines_total", "lines_copied", "lines_changed")
    total = {k: sum(figures[k] for figures in summary.values()) for k in keys}
    lines.append(
        f"{'total':<10}{total['files']:>6}{total['lines_total']:>8}"
        f"{total['lines_copied']:>8}{total['lines_changed']:>9}"
    )
    return "\n".join(lines)


def write(root: Path, tracklab: Path | None) -> Report:
    """Recount and rewrite ``reports/provenance.json`` and the count columns of the table."""
    report = build(root, tracklab, _load_stored(root))
    report_path = root / REPORT
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_bytes(render_json(report).encode("utf-8"))
    doc_path = root / DOC
    doc_path.write_bytes(fill_table(_read(doc_path), report).encode("utf-8"))
    return report


def check(root: Path, tracklab: Path | None) -> list[str]:
    """What differs from the files as they are now; an empty list means they agree.

    Without ``tracklab`` every file is recounted against this repository's import commit (a file
    whose origins it does not hold keeps a stored count that still matches). With ``tracklab``
    every file is recounted there.
    """
    try:
        report = build(root, tracklab, _load_stored(root))
    except ProvenanceError as exc:
        return str(exc).splitlines()
    problems: list[str] = []
    if _read(root / REPORT) != render_json(report):
        problems.append(f"{REPORT} differs from the files; regenerate it with ftbase provenance")
    if _read(root / DOC) != fill_table(_read(root / DOC), report):
        problems.append(f"the count columns of {DOC} differ from the files")
    return problems
