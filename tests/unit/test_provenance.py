# New in ftbase (no tracklab origin): tests of `ftbase provenance`.
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

from ftbase import provenance
from ftbase.cli import main

ROOT = Path(__file__).resolve().parents[2]

HEADER = (
    "| File in ftbase | Path in tracklab | tracklab commit | State "
    "| Lines copied | Lines changed |\n"
    "|---|---|---|---|---|---|\n"
)
TABLE = (
    HEADER + "| `src/ftbase/a.py` | - | - | nuevo | | |\n"
    "| `tests/test_a.py` | - | - | nuevo | | |\n"
)


def test_every_file_under_src_ftbase_has_a_row() -> None:
    doc = (ROOT / provenance.DOC).read_text(encoding="utf-8")
    in_table = {row.path for row in provenance.read_rows(doc)}
    missing = [p for p in provenance.scope_files(ROOT) if p not in in_table]
    assert missing == []


HEADER_WORD = {"copiado": "Copied from", "reescrito": "Rewritten from", "nuevo": "New in ftbase"}


def test_every_header_says_what_the_table_says() -> None:
    doc = (ROOT / provenance.DOC).read_text(encoding="utf-8")
    wrong = []
    for row in provenance.read_rows(doc):
        if not row.path.endswith((".py", ".sql")):
            continue  # JSON, CSV and py.typed take no header: the table is their record
        lines = (ROOT / row.path).read_text(encoding="utf-8").splitlines()[:5]
        first = re.sub(r"^(#|--) ", "", lines[0])
        block = " ".join(re.sub(r"^(#|--) ?", "", line) for line in lines)
        ok = first.startswith(HEADER_WORD[row.state]) and lines[0].startswith(("# ", "-- "))
        if row.state != "nuevo":
            ok = ok and f"@{row.commit}" in first and all(o in block for o in row.origins)
        if not ok:
            wrong.append(f"{row.path} ({row.state}): {lines[0]!r}")
    assert wrong == []


def test_json_line_totals_are_wc_l_of_each_file() -> None:
    report = json.loads((ROOT / provenance.REPORT).read_text(encoding="utf-8"))
    for entry in report["files"]:
        data = (ROOT / entry["path"]).read_bytes()
        assert entry["lines_total"] == data.count(b"\n"), entry["path"]


def test_check_fails_after_an_edit_that_is_not_regenerated(tmp_path: Path) -> None:
    (tmp_path / "src/ftbase").mkdir(parents=True)
    (tmp_path / "tests").mkdir()
    (tmp_path / "docs").mkdir()
    (tmp_path / "src/ftbase/a.py").write_bytes(b"x = 1\n\ny = 2\n")
    (tmp_path / "tests/test_a.py").write_bytes(b"def test_a() -> None:\n    pass\n")
    (tmp_path / "docs/provenance.md").write_bytes(TABLE.encode("utf-8"))
    provenance.write(tmp_path, None)
    assert main(["provenance", "--check", "--root", str(tmp_path)]) == 0

    (tmp_path / "src/ftbase/a.py").write_bytes(b"x = 1\n\ny = 3\n")
    assert main(["provenance", "--check", "--root", str(tmp_path)]) == 1
    assert provenance.check(tmp_path, None) == [
        f"{provenance.REPORT} differs from the files; regenerate it with ftbase provenance"
    ]


def test_copied_lines_are_matched_in_order() -> None:
    original = ["a\n", "b\n", "c\n"]
    current = ["a\n", "x\n", "c\n"]
    assert provenance.count_copied(original, current) == 2


def test_recount_against_tracklab_uses_git_show(tmp_path: Path) -> None:
    tracklab = tmp_path / "tracklab"
    tracklab.mkdir()
    subprocess.run(["git", "-C", str(tracklab), "init", "-q"], check=True)
    (tracklab / "old.py").write_bytes(b"a\nb\nc\n")
    subprocess.run(["git", "-C", str(tracklab), "add", "old.py"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(tracklab),
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-q",
            "-m",
            "origin",
        ],
        check=True,
    )
    commit = subprocess.run(
        ["git", "-C", str(tracklab), "rev-parse", "--short", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    root = tmp_path / "base"
    (root / "src/ftbase").mkdir(parents=True)
    (root / "tests").mkdir()
    (root / "docs").mkdir()
    (root / "src/ftbase/new.py").write_bytes(b"x\nb\nc\n")
    (root / "tests/test_new.py").write_bytes(b"")
    (root / "docs/provenance.md").write_bytes(
        (
            HEADER + f"| `src/ftbase/new.py` | `old.py` | `{commit}` | reescrito | | |\n"
            "| `tests/test_new.py` | - | - | nuevo | | |\n"
        ).encode("utf-8")
    )
    report = provenance.write(root, tracklab)
    entry = {e.row.path: e for e in report.entries}["src/ftbase/new.py"]
    assert (entry.lines_copied, entry.lines_changed) == (2, 1)


# --- regenerating without tracklab ---------------------------------------------------------

ORIGINAL = "".join(f"line {n}\n" for n in range(10))


def _git(repo: Path, *args: str) -> str:
    base = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid"]
    done = subprocess.run([*base, *args], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def _tracklab(tmp_path: Path) -> tuple[Path, str]:
    """A tracklab stand-in holding the two originals, and its commit."""
    tracklab = tmp_path / "tracklab"
    (tracklab / "pipeline").mkdir(parents=True)
    _git(tracklab, "init", "-q")
    (tracklab / "pipeline/old.py").write_bytes(ORIGINAL.encode())
    (tracklab / "pipeline/gone.py").write_bytes(b"a\nb\nz\n")
    _git(tracklab, "add", "-A")
    _git(tracklab, "commit", "-q", "-m", "origin")
    return tracklab, _git(tracklab, "rev-parse", "--short", "HEAD")


def _contributor_repo(tmp_path: Path, tracklab_commit: str) -> Path:
    """A repo like ftbase: an import commit holding one original, then the files in place.

    `src/ftbase/moved.py` is `copiado` from `pipeline/old.py` (in the import commit);
    `src/ftbase/outside.py` is `reescrito` from `pipeline/gone.py` (not in it).
    """
    repo = tmp_path / "base"
    (repo / "pipeline").mkdir(parents=True)
    _git(repo, "init", "-q")
    (repo / "pipeline/old.py").write_bytes(ORIGINAL.encode())
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", f"Import football base from tracklab@{tracklab_commit} (x)")
    for folder in ("src/ftbase", "tests", "docs"):
        (repo / folder).mkdir(parents=True)
    (repo / "pipeline/old.py").rename(repo / "src/ftbase/moved.py")
    (repo / "src/ftbase/outside.py").write_bytes(b"a\nb\nc\n")
    (repo / "src/ftbase/fresh.py").write_bytes(b"x = 1\n")
    (repo / "tests/test_fresh.py").write_bytes(b"")
    c = tracklab_commit
    rows = [
        f"| `src/ftbase/moved.py` | `pipeline/old.py` | `{c}` | copiado | | |",
        f"| `src/ftbase/outside.py` | `pipeline/gone.py` | `{c}` | reescrito | | |",
        "| `src/ftbase/fresh.py` | - | - | nuevo | | |",
        "| `tests/test_fresh.py` | - | - | nuevo | | |",
    ]
    (repo / "docs/provenance.md").write_bytes((HEADER + "\n".join(rows) + "\n").encode())
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "Move the file and add the others")
    return repo


def _regenerated(tmp_path: Path) -> tuple[Path, str]:
    tracklab, commit = _tracklab(tmp_path)
    repo = _contributor_repo(tmp_path, commit)
    provenance.write(repo, tracklab)
    return repo, commit


def _stored(repo: Path) -> dict[str, dict[str, int]]:
    files = json.loads((repo / provenance.REPORT).read_text(encoding="utf-8"))["files"]
    return {f["path"]: f for f in files}


def test_a_new_line_in_a_nuevo_file_regenerates_without_tracklab(tmp_path: Path) -> None:
    repo, _ = _regenerated(tmp_path)
    before = _stored(repo)
    (repo / "src/ftbase/fresh.py").write_bytes(b"x = 1\ny = 2\n")
    assert main(["provenance", "--check", "--root", str(repo)]) == 1

    assert main(["provenance", "--root", str(repo)]) == 0

    after = _stored(repo)
    assert (
        after["src/ftbase/fresh.py"]["lines_total"],
        after["src/ftbase/fresh.py"]["lines_copied"],
    ) == (2, 0)
    for untouched in ("src/ftbase/moved.py", "src/ftbase/outside.py"):
        assert after[untouched] == before[untouched], "an unchanged file keeps its stored count"
    assert main(["provenance", "--check", "--root", str(repo)]) == 0


def test_an_edited_copiado_file_is_recounted_against_the_import_commit(tmp_path: Path) -> None:
    repo, _ = _regenerated(tmp_path)
    edited = ORIGINAL.replace("line 3\n", "") + "added\n"
    (repo / "src/ftbase/moved.py").write_bytes(edited.encode())

    assert main(["provenance", "--root", str(repo)]) == 0

    entry = _stored(repo)["src/ftbase/moved.py"]
    assert (entry["lines_total"], entry["lines_copied"], entry["lines_changed"]) == (10, 9, 1)
    assert main(["provenance", "--check", "--root", str(repo)]) == 0


def test_an_edited_file_with_no_reachable_import_says_what_is_missing(tmp_path: Path) -> None:
    repo, commit = _regenerated(tmp_path)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "Record provenance")
    shallow = tmp_path / "shallow"
    subprocess.run(
        [
            "git",
            "clone",
            "-q",
            "-c",
            "core.autocrlf=false",
            "--depth",
            "1",
            repo.as_uri(),
            str(shallow),
        ],
        check=True,
    )
    (shallow / "src/ftbase/moved.py").write_bytes(b"changed\n")

    problems = provenance.check(shallow, None)

    assert len(problems) == 1
    assert f"Import football base from tracklab@{commit}" in problems[0]
    assert "--tracklab" in problems[0]


def test_an_edited_file_whose_origin_is_outside_the_import_asks_for_tracklab(
    tmp_path: Path,
) -> None:
    repo, _ = _regenerated(tmp_path)
    (repo / "src/ftbase/outside.py").write_bytes(b"a\nb\nc\nd\n")

    problems = provenance.check(repo, None)

    assert len(problems) == 1
    assert "pipeline/gone.py is not in the import commit" in problems[0]
    with pytest.raises(SystemExit, match="not in the import commit"):
        main(["provenance", "--root", str(repo)])
