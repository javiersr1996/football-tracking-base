# New in ftbase (no tracklab origin).
"""A skipped test is a failure here.

The only tests that need the real bundle carry the ``realdata`` marker and are deselected
(``addopts`` in ``pyproject.toml``), so nothing in the gate has a reason to skip. A skip
would mean a test that no longer runs and no longer protects anything: the session ends
with a failing exit status and names what was skipped.
"""

from __future__ import annotations

import pytest


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    skipped = reporter.stats.get("skipped", []) if reporter is not None else []
    if skipped:
        names = ", ".join(sorted({getattr(r, "nodeid", str(r)) for r in skipped}))
        if reporter is not None:
            reporter.write_line(
                f"FAILED: {len(skipped)} skipped test(s) count as failures: {names}"
            )
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
