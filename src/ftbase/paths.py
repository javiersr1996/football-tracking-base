"""The data root, from one environment variable shared with dbt.

`TRACKLAB_DATA_ROOT` moves raw, bronze, silver and the DuckDB file together:
dbt_project.yml, profiles.yml and the sources read the same variable, so CI
builds on the fixtures by setting it once instead of passing a flag to every
command. Unset, it is `data`, relative to pipeline/ like every other path.
"""

from __future__ import annotations

import os
from pathlib import Path

DATA_ROOT_ENV = "TRACKLAB_DATA_ROOT"


def data_root() -> Path:
    return Path(os.environ.get(DATA_ROOT_ENV, "data"))
