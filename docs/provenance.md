# Provenance

Every file imported from tracklab (the author's private repository, MIT) at commit `686508a`.
The line columns are filled in by task 7.

The ingestion rows already point at their new location. `src/ftbase/manifest/sources.json` is the tracklab
manifest cut to its `opendata-football` source (85 files, commit `4340d27`); JSON takes no header, so
this table is its record. `select.py` holds `--match`, which has no tracklab origin. `cli.py` is the
tracklab CLI rewritten down to the `ingest` and `bronze` subcommands. `test_football_events_bronze.py`
gains a header-hash test and `test_frozen_schemas.py` loses basketball (both `reescrito`);
`manifest_helper.py` replaces the `save_manifest` that tracklab's manifest module had.

Silver (task 5) replaces dbt with DuckDB run from Python, football only. The model SQL keeps its
logic; `reescrito` marks the files that lost their basketball branch or changed a source, `copiado`
the ones where only dbt's `ref()`/`source()` became a table name. `checks.py` holds the integrity
tests of `schema.yml` (key uniqueness, not-null keys, relationships) and replaces the
`unique_combination` macro; `build.py` takes the place of the dbt sources, seeds and run order.
`event_type_map.csv` keeps its 3 football rows (CSV takes no header, so this table is its record;
one note now points at `events.sql` instead of a tracklab report). Not carried over: the
`surface_spec.csv` seed (basketball courts only), `schema.yml`, `sources/{football,common}.yml` and
the macro, whose content moved into the files above. `orientation.py` is the football part of
tracklab's `silver/orientation.py` (the sign from `home_team_side` and its check against the phases),
without its basketball, event-frame probe, reports or docs. `read.py` (the typed readers and the
per-connection views) has no tracklab origin: tracklab read silver through the views dbt left in its
DuckDB file. `pipeline/` no longer exists; its last file, `pipeline/src/tracklab/__init__.py`, was
the origin of `src/ftbase/__init__.py` and is removed.

Task 6 adds `fixtures.py` (the synthetic match and `ftbase fixtures`, which also touches `cli.py`),
its end-to-end test, and a `conftest.py` that makes a skipped test a failure (with its own test).
`.github/workflows/ci.yml` is new and sits outside this table. `checks.py` gains two `not_null`
checks (`frames.is_detected`, `frames_attacking.attack_sign`) that `schema.yml` had and task 5 left out.

| File in ftbase | Path in tracklab | tracklab commit | State | Lines copied | Lines changed |
|---|---|---|---|---|---|
| `src/ftbase/silver/sql/dim_entity.sql` | `pipeline/dbt/models/silver/dim_entity.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/dim_match.sql` | `pipeline/dbt/models/silver/dim_match.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/dim_period.sql` | `pipeline/dbt/models/silver/dim_period.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/dim_sport.sql` | `pipeline/dbt/models/silver/dim_sport.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/dim_team.sql` | `pipeline/dbt/models/silver/dim_team.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/dim_team_period.sql` | `pipeline/dbt/models/silver/dim_team_period.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/sql/events.sql` | `pipeline/dbt/models/silver/events.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/frames.sql` | `pipeline/dbt/models/silver/frames.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/sql/frame_possession.sql` | `pipeline/dbt/models/silver/views/frame_possession.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/sql/frames_attacking.sql` | `pipeline/dbt/models/silver/views/frames_attacking.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/seeds/event_type_map.csv` | `pipeline/dbt/seeds/event_type_map.csv` | `686508a` | reescrito | | |
| `src/ftbase/silver/assertions/assert_attack_signs_opposite.sql` | `pipeline/dbt/tests/assert_attack_signs_opposite.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/assertions/assert_football_possessions_do_not_overlap.sql` | `pipeline/dbt/tests/assert_football_possessions_do_not_overlap.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/assertions/assert_frames_entities_in_dim_entity.sql` | `pipeline/dbt/tests/assert_frames_entities_in_dim_entity.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/assertions/assert_frames_t_ms_increases.sql` | `pipeline/dbt/tests/assert_frames_t_ms_increases.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/assertions/assert_frames_z_only_on_the_ball.sql` | `pipeline/dbt/tests/assert_frames_z_only_on_the_ball.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/assertions/assert_one_ball_per_match.sql` | `pipeline/dbt/tests/assert_one_ball_per_match.sql` | `686508a` | copiado | | |
| `src/ftbase/silver/checks.py` | `pipeline/dbt/models/silver/schema.yml` and `pipeline/dbt/macros/test_unique_combination.sql` | `686508a` | reescrito | | |
| `src/ftbase/silver/build.py` | `pipeline/dbt/models/sources/football.yml` and `pipeline/dbt/models/sources/common.yml` | `686508a` | reescrito | | |
| `src/ftbase/silver/orientation.py` | `pipeline/src/tracklab/silver/orientation.py` | `686508a` | reescrito | | |
| `src/ftbase/fixtures.py` | - | - | nuevo | | |
| `tests/unit/test_fixtures.py` | - | - | nuevo | | |
| `tests/conftest.py` | - | - | nuevo | | |
| `tests/test_skips_fail.py` | - | - | nuevo | | |
| `src/ftbase/silver/read.py` | - | - | nuevo | | |
| `src/ftbase/silver/__init__.py` | - | - | nuevo | | |
| `tests/unit/test_silver_orientation.py` | `pipeline/tests/unit/test_silver_orientation.py` | `686508a` | reescrito | | |
| `tests/unit/test_silver_build.py` | - | - | nuevo | | |
| `tests/contracts/test_silver.py` | - | - | nuevo | | |
| `src/ftbase/manifest/sources.json` | `pipeline/manifest/sources.json` | `686508a` | copiado | | |
| `src/ftbase/__init__.py` | `pipeline/src/tracklab/__init__.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/__init__.py` | `pipeline/src/tracklab/bronze/__init__.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/common.py` | `pipeline/src/tracklab/bronze/common.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/football/__init__.py` | `pipeline/src/tracklab/bronze/football/__init__.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/football/events.py` | `pipeline/src/tracklab/bronze/football/events.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/football/meta.py` | `pipeline/src/tracklab/bronze/football/meta.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/football/tracking.py` | `pipeline/src/tracklab/bronze/football/tracking.py` | `686508a` | copiado | | |
| `src/ftbase/bronze/schemas/football/dynamic_events.schema.json` | `pipeline/src/tracklab/bronze/schemas/football/dynamic_events.schema.json` | `686508a` | copiado | | |
| `src/ftbase/bronze/schemas/football/phases_of_play.schema.json` | `pipeline/src/tracklab/bronze/schemas/football/phases_of_play.schema.json` | `686508a` | copiado | | |
| `src/ftbase/formats/__init__.py` | `pipeline/src/tracklab/formats/__init__.py` | `686508a` | copiado | | |
| `src/ftbase/formats/csvschema.py` | `pipeline/src/tracklab/formats/csvschema.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/__init__.py` | `pipeline/src/tracklab/ingest/__init__.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/fetch.py` | `pipeline/src/tracklab/ingest/fetch.py` | `686508a` | reescrito | | |
| `src/ftbase/ingest/lfs.py` | `pipeline/src/tracklab/ingest/lfs.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/local.py` | `pipeline/src/tracklab/ingest/local.py` | `686508a` | reescrito | | |
| `src/ftbase/ingest/manifest.py` | `pipeline/src/tracklab/ingest/manifest.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/verify.py` | `pipeline/src/tracklab/ingest/verify.py` | `686508a` | copiado | | |
| `src/ftbase/paths.py` | `pipeline/src/tracklab/paths.py` | `686508a` | copiado | | |
| `tests/contracts/test_football_events_bronze.py` | `pipeline/tests/contracts/test_football_events_bronze.py` | `686508a` | reescrito | | |
| `tests/contracts/test_football_meta_bronze.py` | `pipeline/tests/contracts/test_football_meta_bronze.py` | `686508a` | copiado | | |
| `tests/contracts/test_football_tracking_bronze.py` | `pipeline/tests/contracts/test_football_tracking_bronze.py` | `686508a` | copiado | | |
| `tests/unit/test_bronze_football_events.py` | `pipeline/tests/unit/test_bronze_football_events.py` | `686508a` | copiado | | |
| `tests/unit/test_bronze_football_meta.py` | `pipeline/tests/unit/test_bronze_football_meta.py` | `686508a` | copiado | | |
| `tests/unit/test_bronze_football_tracking.py` | `pipeline/tests/unit/test_bronze_football_tracking.py` | `686508a` | copiado | | |
| `tests/unit/test_frozen_schemas.py` | `pipeline/tests/unit/test_frozen_schemas.py` | `686508a` | reescrito | | |
| `tests/unit/test_ingest.py` | `pipeline/tests/unit/test_ingest.py` | `686508a` | reescrito | | |
| `src/ftbase/ingest/select.py` | - | - | nuevo | | |
| `src/ftbase/cli.py` | `pipeline/src/tracklab/cli.py` | `686508a` | reescrito | | |
| `tests/unit/manifest_helper.py` | - | - | nuevo | | |
