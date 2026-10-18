# Copied from tracklab@686508a: pipeline/src/tracklab/paths.py
# Changes: env var renamed to FTBASE_DATA_ROOT; docstring no longer mentions dbt.
"""The data root, from one environment variable.

`FTBASE_DATA_ROOT` moves raw, bronze and silver together, so CI builds on the
fixtures by setting it once instead of passing a flag to every command. Unset,
it is `data`, relative to the working directory (the repository root, where
`data/` is git-ignored).
"""

from __future__ import annotations

import os
from pathlib import Path

DATA_ROOT_ENV = "FTBASE_DATA_ROOT"


def data_root() -> Path:
    return Path(os.environ.get(DATA_ROOT_ENV, "data"))
