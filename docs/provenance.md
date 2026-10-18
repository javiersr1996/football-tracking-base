# Provenance

The import commit (`Import football base from tracklab@686508a`) holds 49 files of tracklab
(the author's private repository, MIT) at commit `686508a`. 47 of them are byte-identical to
tracklab; in the other two, comment lines were withdrawn before this repository was made public
(`pipeline/dbt/models/silver/frames.sql`: six lines replaced by two;
`pipeline/dbt/models/silver/schema.yml`: one sentence of a column description). No code or SQL
statement changed. The table has one row for every
file of this repository under `src/ftbase/` and `tests/`. The ones marked `copiado` started as
literal copies of imported files. On origins:

- 48 of the 49 imported files are the origin of a row. The 49th, the basketball-only seed
  `pipeline/dbt/seeds/surface_spec.csv`, was not carried over, so no file here can have a row
  for it.
- Three origins were **not** in the import commit; they were taken from tracklab@686508a directly:
  `cli.py`, `silver/orientation.py` and `tests/unit/test_silver_orientation.py`. Their rows
  say so, and recounting them needs `--tracklab`.
- `nuevo` rows have no origin.

## Scope and counting

- **Scope:** every file under `src/ftbase/` and `tests/` (`__pycache__` and `*.pyc` aside). Each
  one has a row. `.github/workflows/ci.yml` and this file are outside it.
- **Line:** what `wc -l` counts, a text ending in LF (CRLF is read as LF). Every file in scope ends
  in LF or is empty.
- **Lines copied:** the lines of the current file that `difflib.SequenceMatcher` (with
  `autojunk=False`, so no line is ignored as junk) matches in order to the original. The original
  is `git show <commit>:<path>` in tracklab for each path in the second column, concatenated in the
  order the column lists them. **Lines changed:** every other line. A `nuevo` file has no origin:
  nothing copied, every line changed.
- **`ftbase provenance`** regenerates `reports/provenance.json` (sorted keys, LF, no dates) and the
  two count columns below. Where the originals come from decides what it can count:
  - `nuevo` row: nothing to read, every line is changed. Works anywhere.
  - Any other row: recounted against the original as it sits in this repository's own import
    commit, found by its message (`git log --grep='^Import football base from
    tracklab@<commit>'`, never by a fixed sha, so a rewritten history still works). The import
    commit holds the files at their tracklab paths, so the origin column is the path to read.
  - A row whose origin that commit does not hold (`cli.py`, `orientation.py` and
    `test_silver_orientation.py` were written after the import), or a clone where the commit is
    not reachable (a shallow clone): the stored count is reused only if the sha256, the origin
    paths, the commit and the state stored in `reports/provenance.json` equal the file's and the
    row's. Otherwise the command exits 1 and names the file and what is missing.
  - **`--tracklab <path>`**: every row is recounted against that clone with `git show` (read
    only), whatever the JSON says. Use it after editing one of those three files, or to
    cross-check the offline counts.
- **`ftbase provenance --check`** recomputes the scope, the rows, the sha256 and line count of each
  file and the states, with the same sources as above, and fails if `reports/provenance.json` or
  the count columns differ. With `--tracklab` it recounts everything there. The gate runs it last
  in `ci.yml`, without tracklab.


The ingestion rows already point at their new location. `src/ftbase/manifest/sources.json` is the tracklab
manifest cut to its `opendata-football` source (85 files, commit `4340d27`); JSON takes no header, so
this table is its record. `select.py` holds `--match`, which has no tracklab origin. `cli.py` is the
tracklab CLI rewritten down to football's subcommands (`ingest`, `bronze`, `silver`) plus the new
`fixtures` and `provenance`. `test_football_events_bronze.py` gains a header-hash test and
`test_frozen_schemas.py` loses basketball (both `reescrito`); `manifest_helper.py` is `copiado`:
its `save_manifest` is lines 117-119 of tracklab's `ingest/manifest.py` verbatim, lifted into a
test helper because ftbase's manifest module no longer saves.

Silver replaces dbt with DuckDB run from Python, football only. The model SQL keeps its
logic; `reescrito` marks the files that lost their basketball branch or changed a source, `copiado`
the ones where only dbt's `ref()`/`source()` became a table name. `checks.py` holds the integrity
tests of `schema.yml` (key uniqueness, not-null keys, relationships) and replaces the
`unique_combination` macro; `build.py` takes the place of the dbt sources, seeds and run order.
`event_type_map.csv` keeps its 3 football rows (CSV takes no header, so this table is its record;
one note now points at `events.sql` instead of a tracklab report). Not carried over: the
`surface_spec.csv` seed (basketball courts only). `schema.yml`, `sources/{football,common}.yml` and
the macro are not files here either: their content moved into `checks.py` and `build.py`. `orientation.py` is the football part of
tracklab's `silver/orientation.py` (the sign from `home_team_side` and its check against the phases),
without its basketball, event-frame probe, reports or docs. `read.py` (the typed readers and the
per-connection views) has no tracklab origin: tracklab read silver through the views dbt left in its
DuckDB file. `pipeline/` no longer exists; its last file, `pipeline/src/tracklab/__init__.py`, was
the origin of `src/ftbase/__init__.py` and is removed.

The synthetic fixtures add `fixtures.py` (the synthetic match and `ftbase fixtures`, which also touches `cli.py`),
its end-to-end test, and a `conftest.py` that makes a skipped test a failure (with its own test).
`.github/workflows/ci.yml` is new and sits outside this table. `checks.py` gains two `not_null`
checks (`frames.is_detected`, `frames_attacking.attack_sign`) that `schema.yml` had and the first silver build left out.

| File in ftbase | Path in tracklab | tracklab commit | State | Lines copied | Lines changed |
|---|---|---|---|---|---|
| `src/ftbase/silver/sql/dim_entity.sql` | `pipeline/dbt/models/silver/dim_entity.sql` | `686508a` | reescrito | 44 | 20 |
| `src/ftbase/silver/sql/dim_match.sql` | `pipeline/dbt/models/silver/dim_match.sql` | `686508a` | reescrito | 49 | 25 |
| `src/ftbase/silver/sql/dim_period.sql` | `pipeline/dbt/models/silver/dim_period.sql` | `686508a` | reescrito | 21 | 14 |
| `src/ftbase/silver/sql/dim_sport.sql` | `pipeline/dbt/models/silver/dim_sport.sql` | `686508a` | reescrito | 3 | 16 |
| `src/ftbase/silver/sql/dim_team.sql` | `pipeline/dbt/models/silver/dim_team.sql` | `686508a` | reescrito | 19 | 13 |
| `src/ftbase/silver/sql/dim_team_period.sql` | `pipeline/dbt/models/silver/dim_team_period.sql` | `686508a` | copiado | 7 | 13 |
| `src/ftbase/silver/sql/events.sql` | `pipeline/dbt/models/silver/events.sql` | `686508a` | reescrito | 146 | 45 |
| `src/ftbase/silver/sql/frames.sql` | `pipeline/dbt/models/silver/frames.sql` | `686508a` | reescrito | 63 | 46 |
| `src/ftbase/silver/sql/frame_possession.sql` | `pipeline/dbt/models/silver/views/frame_possession.sql` | `686508a` | copiado | 23 | 13 |
| `src/ftbase/silver/sql/frames_attacking.sql` | `pipeline/dbt/models/silver/views/frames_attacking.sql` | `686508a` | copiado | 16 | 10 |
| `src/ftbase/silver/seeds/event_type_map.csv` | `pipeline/dbt/seeds/event_type_map.csv` | `686508a` | reescrito | 3 | 1 |
| `src/ftbase/silver/assertions/assert_attack_signs_opposite.sql` | `pipeline/dbt/tests/assert_attack_signs_opposite.sql` | `686508a` | copiado | 8 | 9 |
| `src/ftbase/silver/assertions/assert_football_possessions_do_not_overlap.sql` | `pipeline/dbt/tests/assert_football_possessions_do_not_overlap.sql` | `686508a` | copiado | 14 | 8 |
| `src/ftbase/silver/assertions/assert_frames_entities_in_dim_entity.sql` | `pipeline/dbt/tests/assert_frames_entities_in_dim_entity.sql` | `686508a` | copiado | 5 | 8 |
| `src/ftbase/silver/assertions/assert_frames_t_ms_increases.sql` | `pipeline/dbt/tests/assert_frames_t_ms_increases.sql` | `686508a` | copiado | 11 | 8 |
| `src/ftbase/silver/assertions/assert_frames_z_only_on_the_ball.sql` | `pipeline/dbt/tests/assert_frames_z_only_on_the_ball.sql` | `686508a` | copiado | 5 | 8 |
| `src/ftbase/silver/assertions/assert_one_ball_per_match.sql` | `pipeline/dbt/tests/assert_one_ball_per_match.sql` | `686508a` | copiado | 10 | 5 |
| `src/ftbase/silver/checks.py` | `pipeline/dbt/models/silver/schema.yml` and `pipeline/dbt/macros/test_unique_combination.sql` | `686508a` | reescrito | 14 | 132 |
| `src/ftbase/silver/build.py` | `pipeline/dbt/models/sources/football.yml` and `pipeline/dbt/models/sources/common.yml` | `686508a` | reescrito | 4 | 174 |
| `src/ftbase/silver/orientation.py` | `pipeline/src/tracklab/silver/orientation.py` (not in the import commit) | `686508a` | reescrito | 117 | 59 |
| `src/ftbase/fixtures.py` | - | - | nuevo | 0 | 533 |
| `tests/unit/test_fixtures.py` | - | - | nuevo | 0 | 297 |
| `tests/conftest.py` | - | - | nuevo | 0 | 24 |
| `tests/test_skips_fail.py` | - | - | nuevo | 0 | 38 |
| `tests/test_cli.py` | - | - | nuevo | 0 | 23 |
| `tests/unit/test_provenance.py` | - | - | nuevo | 0 | 306 |
| `src/ftbase/silver/read.py` | - | - | nuevo | 0 | 212 |
| `src/ftbase/silver/__init__.py` | - | - | nuevo | 0 | 10 |
| `tests/unit/test_silver_orientation.py` | `pipeline/tests/unit/test_silver_orientation.py` (not in the import commit) | `686508a` | reescrito | 119 | 57 |
| `tests/unit/test_silver_build.py` | - | - | nuevo | 0 | 472 |
| `tests/contracts/test_silver.py` | - | - | nuevo | 0 | 77 |
| `src/ftbase/manifest/sources.json` | `pipeline/manifest/sources.json` | `686508a` | copiado | 698 | 0 |
| `src/ftbase/__init__.py` | `pipeline/src/tracklab/__init__.py` | `686508a` | copiado | 2 | 3 |
| `src/ftbase/bronze/__init__.py` | `pipeline/src/tracklab/bronze/__init__.py` | `686508a` | copiado | 5 | 3 |
| `src/ftbase/bronze/common.py` | `pipeline/src/tracklab/bronze/common.py` | `686508a` | copiado | 207 | 20 |
| `src/ftbase/bronze/football/__init__.py` | `pipeline/src/tracklab/bronze/football/__init__.py` | `686508a` | copiado | 1 | 2 |
| `src/ftbase/bronze/football/events.py` | `pipeline/src/tracklab/bronze/football/events.py` | `686508a` | copiado | 126 | 26 |
| `src/ftbase/bronze/football/meta.py` | `pipeline/src/tracklab/bronze/football/meta.py` | `686508a` | copiado | 670 | 33 |
| `src/ftbase/bronze/football/tracking.py` | `pipeline/src/tracklab/bronze/football/tracking.py` | `686508a` | copiado | 233 | 24 |
| `src/ftbase/bronze/schemas/football/dynamic_events.schema.json` | `pipeline/src/tracklab/bronze/schemas/football/dynamic_events.schema.json` | `686508a` | copiado | 1321 | 0 |
| `src/ftbase/bronze/schemas/football/phases_of_play.schema.json` | `pipeline/src/tracklab/bronze/schemas/football/phases_of_play.schema.json` | `686508a` | copiado | 209 | 0 |
| `src/ftbase/formats/__init__.py` | `pipeline/src/tracklab/formats/__init__.py` | `686508a` | copiado | 2 | 6 |
| `src/ftbase/formats/csvschema.py` | `pipeline/src/tracklab/formats/csvschema.py` | `686508a` | copiado | 191 | 2 |
| `src/ftbase/ingest/__init__.py` | `pipeline/src/tracklab/ingest/__init__.py` | `686508a` | copiado | 0 | 3 |
| `src/ftbase/ingest/fetch.py` | `pipeline/src/tracklab/ingest/fetch.py` | `686508a` | reescrito | 140 | 32 |
| `src/ftbase/ingest/lfs.py` | `pipeline/src/tracklab/ingest/lfs.py` | `686508a` | copiado | 36 | 2 |
| `src/ftbase/ingest/local.py` | `pipeline/src/tracklab/ingest/local.py` | `686508a` | reescrito | 47 | 28 |
| `src/ftbase/ingest/manifest.py` | `pipeline/src/tracklab/ingest/manifest.py` | `686508a` | copiado | 84 | 6 |
| `src/ftbase/ingest/verify.py` | `pipeline/src/tracklab/ingest/verify.py` | `686508a` | copiado | 35 | 2 |
| `src/ftbase/paths.py` | `pipeline/src/tracklab/paths.py` | `686508a` | copiado | 12 | 8 |
| `src/ftbase/py.typed` | - | - | nuevo | 0 | 0 |
| `src/ftbase/provenance.py` | - | - | nuevo | 0 | 379 |
| `tests/contracts/test_football_events_bronze.py` | `pipeline/tests/contracts/test_football_events_bronze.py` | `686508a` | reescrito | 165 | 42 |
| `tests/contracts/test_football_meta_bronze.py` | `pipeline/tests/contracts/test_football_meta_bronze.py` | `686508a` | copiado | 215 | 19 |
| `tests/contracts/test_football_tracking_bronze.py` | `pipeline/tests/contracts/test_football_tracking_bronze.py` | `686508a` | copiado | 165 | 17 |
| `tests/unit/test_bronze_football_events.py` | `pipeline/tests/unit/test_bronze_football_events.py` | `686508a` | copiado | 287 | 13 |
| `tests/unit/test_bronze_football_meta.py` | `pipeline/tests/unit/test_bronze_football_meta.py` | `686508a` | copiado | 446 | 10 |
| `tests/unit/test_bronze_football_tracking.py` | `pipeline/tests/unit/test_bronze_football_tracking.py` | `686508a` | copiado | 332 | 15 |
| `tests/unit/test_frozen_schemas.py` | `pipeline/tests/unit/test_frozen_schemas.py` | `686508a` | reescrito | 74 | 38 |
| `tests/unit/test_ingest.py` | `pipeline/tests/unit/test_ingest.py` | `686508a` | reescrito | 209 | 226 |
| `src/ftbase/ingest/select.py` | - | - | nuevo | 0 | 24 |
| `src/ftbase/cli.py` | `pipeline/src/tracklab/cli.py` (not in the import commit) | `686508a` | reescrito | 106 | 167 |
| `tests/unit/manifest_helper.py` | `pipeline/src/tracklab/ingest/manifest.py` | `686508a` | copiado | 11 | 7 |
