# New in ftbase (no tracklab origin).
"""The skip-is-a-failure hook of ``conftest.py`` really changes the exit status."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

CONFTEST = Path(__file__).parent / "conftest.py"


def run_pytest(folder: Path, body: str) -> subprocess.CompletedProcess[str]:
    shutil.copy(CONFTEST, folder / "conftest.py")
    (folder / "test_probe.py").write_text(body, encoding="utf-8", newline="\n")
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(folder)],
        capture_output=True,
        text=True,
        cwd=folder,
        check=False,
    )


def test_a_skipped_test_fails_the_session(tmp_path: Path) -> None:
    result = run_pytest(tmp_path, "import pytest\n\ndef test_a():\n    pytest.skip('probe')\n")
    assert result.returncode == 1, result.stdout
    assert "1 skipped test(s) count as failures" in result.stdout


def test_a_skip_by_marker_fails_it_too(tmp_path: Path) -> None:
    body = "import pytest\n\n@pytest.mark.skipif(True, reason='x')\ndef test_a():\n    pass\n"
    assert run_pytest(tmp_path, body).returncode == 1


def test_passing_tests_keep_exit_zero(tmp_path: Path) -> None:
    assert run_pytest(tmp_path, "def test_a():\n    assert True\n").returncode == 0
