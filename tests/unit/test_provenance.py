from __future__ import annotations

import json
import subprocess
from pathlib import Path

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
