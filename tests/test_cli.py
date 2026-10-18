# New in ftbase (no tracklab origin): the command-line entry point.
import pytest

from ftbase import __version__
from ftbase.cli import main


def test_help_exits_zero_and_names_the_program(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "usage: ftbase" in capsys.readouterr().out


def test_version_prints_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"ftbase {__version__}"


def test_no_arguments_returns_zero() -> None:
    assert main([]) == 0
