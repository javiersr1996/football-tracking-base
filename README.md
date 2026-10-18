# ftbase

Open pipeline for the SkillCorner open football data (A-League 2024/25: tracking, dynamic events,
phases of play). It was extracted in October 2026 from tracklab, the author's private repository,
which is where the code was first written (MIT). The pipeline keeps the provider's `is_detected`
flag, which separates what the cameras saw from what the provider extrapolated.

**Status: pre-release.** `ingest`, `bronze` and `silver` are in place; v0.1.0 is not tagged yet.

## Install

```
uv sync --locked
uv run ftbase --version
```

Python 3.13 is pinned in `.python-version`; uv installs it.

## Commands

- `ftbase ingest`: download and verify the 85 source files against a sha256 manifest (`--match <id>`
  for one match and `matches.json`, about 91 MB; `--from-existing <dir>` to verify a local clone
  in place, read-only). Set `FTBASE_DATA_ROOT` or `--data-dir` to move `data/`.
- `ftbase bronze`: write typed Parquet tables (tracking, events, match metadata) under
  `<data root>/bronze/football/`, one file per match, from what `ingest` verified. The contract tests
  that read the real bundle are deselected by default: `uv run pytest -m realdata` runs them.
- `ftbase silver`: build the silver tables with DuckDB (no dbt) under `<data root>/silver/`: the
  dimensions, `frames` (one row per match, frame and entity, in metres, with `is_detected`) and
  `events` (rotated to the tracking frame, with team possessions derived from the provider's own
  flag), one Parquet file per match; the `frames_attacking` view turns the frames of each team
  possession so the team with the ball attacks towards +x. The build runs its checks and exits 1 if any fails. From Python:
  `ftbase.silver.read_frames([match_id])`, `read_events` and `read_dim` (columns in the docstrings).

## Data and LFS

No data is stored in this repository (`data/` is ignored). The tracking files of
`SkillCorner/opendata` are served through Git LFS and are fetched by `ingest`.

## Provenance

See [`docs/provenance.md`](docs/provenance.md): each file, its path in tracklab, and whether it was
copied, rewritten or is new.

## License

MIT, see [`LICENSE.md`](LICENSE.md).
