# Provenance

Every file imported from tracklab (the author's private repository, MIT) at commit `686508a`.
The line columns are filled in by task 7.

The ingestion rows already point at their new location. `src/ftbase/manifest/sources.json` is the tracklab
manifest cut to its `opendata-football` source (85 files, commit `4340d27`); JSON takes no header, so
this table is its record. `select.py` holds `--match`, which has no tracklab origin. `cli.py` is the
tracklab CLI rewritten down to the `ingest` subcommand.

| File in ftbase | Path in tracklab | tracklab commit | State | Lines copied | Lines changed |
|---|---|---|---|---|---|
| `pipeline/dbt/macros/test_unique_combination.sql` | `pipeline/dbt/macros/test_unique_combination.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/dim_entity.sql` | `pipeline/dbt/models/silver/dim_entity.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/dim_match.sql` | `pipeline/dbt/models/silver/dim_match.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/dim_period.sql` | `pipeline/dbt/models/silver/dim_period.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/dim_sport.sql` | `pipeline/dbt/models/silver/dim_sport.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/dim_team.sql` | `pipeline/dbt/models/silver/dim_team.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/dim_team_period.sql` | `pipeline/dbt/models/silver/dim_team_period.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/events.sql` | `pipeline/dbt/models/silver/events.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/frames.sql` | `pipeline/dbt/models/silver/frames.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/schema.yml` | `pipeline/dbt/models/silver/schema.yml` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/views/frame_possession.sql` | `pipeline/dbt/models/silver/views/frame_possession.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/silver/views/frames_attacking.sql` | `pipeline/dbt/models/silver/views/frames_attacking.sql` | `686508a` | copiado | | |
| `pipeline/dbt/models/sources/common.yml` | `pipeline/dbt/models/sources/common.yml` | `686508a` | copiado | | |
| `pipeline/dbt/models/sources/football.yml` | `pipeline/dbt/models/sources/football.yml` | `686508a` | copiado | | |
| `pipeline/dbt/seeds/event_type_map.csv` | `pipeline/dbt/seeds/event_type_map.csv` | `686508a` | copiado | | |
| `pipeline/dbt/seeds/surface_spec.csv` | `pipeline/dbt/seeds/surface_spec.csv` | `686508a` | copiado | | |
| `pipeline/dbt/tests/assert_attack_signs_opposite.sql` | `pipeline/dbt/tests/assert_attack_signs_opposite.sql` | `686508a` | copiado | | |
| `pipeline/dbt/tests/assert_football_possessions_do_not_overlap.sql` | `pipeline/dbt/tests/assert_football_possessions_do_not_overlap.sql` | `686508a` | copiado | | |
| `pipeline/dbt/tests/assert_frames_entities_in_dim_entity.sql` | `pipeline/dbt/tests/assert_frames_entities_in_dim_entity.sql` | `686508a` | copiado | | |
| `pipeline/dbt/tests/assert_frames_t_ms_increases.sql` | `pipeline/dbt/tests/assert_frames_t_ms_increases.sql` | `686508a` | copiado | | |
| `pipeline/dbt/tests/assert_frames_z_only_on_the_ball.sql` | `pipeline/dbt/tests/assert_frames_z_only_on_the_ball.sql` | `686508a` | copiado | | |
| `pipeline/dbt/tests/assert_one_ball_per_match.sql` | `pipeline/dbt/tests/assert_one_ball_per_match.sql` | `686508a` | copiado | | |
| `src/ftbase/manifest/sources.json` | `pipeline/manifest/sources.json` | `686508a` | copiado | | |
| `pipeline/src/tracklab/__init__.py` | `pipeline/src/tracklab/__init__.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/__init__.py` | `pipeline/src/tracklab/bronze/__init__.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/common.py` | `pipeline/src/tracklab/bronze/common.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/football/__init__.py` | `pipeline/src/tracklab/bronze/football/__init__.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/football/events.py` | `pipeline/src/tracklab/bronze/football/events.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/football/meta.py` | `pipeline/src/tracklab/bronze/football/meta.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/football/tracking.py` | `pipeline/src/tracklab/bronze/football/tracking.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/schemas/football/dynamic_events.schema.json` | `pipeline/src/tracklab/bronze/schemas/football/dynamic_events.schema.json` | `686508a` | copiado | | |
| `pipeline/src/tracklab/bronze/schemas/football/phases_of_play.schema.json` | `pipeline/src/tracklab/bronze/schemas/football/phases_of_play.schema.json` | `686508a` | copiado | | |
| `pipeline/src/tracklab/formats/__init__.py` | `pipeline/src/tracklab/formats/__init__.py` | `686508a` | copiado | | |
| `pipeline/src/tracklab/formats/csvschema.py` | `pipeline/src/tracklab/formats/csvschema.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/__init__.py` | `pipeline/src/tracklab/ingest/__init__.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/fetch.py` | `pipeline/src/tracklab/ingest/fetch.py` | `686508a` | reescrito | | |
| `src/ftbase/ingest/lfs.py` | `pipeline/src/tracklab/ingest/lfs.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/local.py` | `pipeline/src/tracklab/ingest/local.py` | `686508a` | reescrito | | |
| `src/ftbase/ingest/manifest.py` | `pipeline/src/tracklab/ingest/manifest.py` | `686508a` | copiado | | |
| `src/ftbase/ingest/verify.py` | `pipeline/src/tracklab/ingest/verify.py` | `686508a` | copiado | | |
| `src/ftbase/paths.py` | `pipeline/src/tracklab/paths.py` | `686508a` | copiado | | |
| `pipeline/tests/contracts/test_football_events_bronze.py` | `pipeline/tests/contracts/test_football_events_bronze.py` | `686508a` | copiado | | |
| `pipeline/tests/contracts/test_football_meta_bronze.py` | `pipeline/tests/contracts/test_football_meta_bronze.py` | `686508a` | copiado | | |
| `pipeline/tests/contracts/test_football_tracking_bronze.py` | `pipeline/tests/contracts/test_football_tracking_bronze.py` | `686508a` | copiado | | |
| `pipeline/tests/unit/test_bronze_football_events.py` | `pipeline/tests/unit/test_bronze_football_events.py` | `686508a` | copiado | | |
| `pipeline/tests/unit/test_bronze_football_meta.py` | `pipeline/tests/unit/test_bronze_football_meta.py` | `686508a` | copiado | | |
| `pipeline/tests/unit/test_bronze_football_tracking.py` | `pipeline/tests/unit/test_bronze_football_tracking.py` | `686508a` | copiado | | |
| `pipeline/tests/unit/test_frozen_schemas.py` | `pipeline/tests/unit/test_frozen_schemas.py` | `686508a` | copiado | | |
| `tests/unit/test_ingest.py` | `pipeline/tests/unit/test_ingest.py` | `686508a` | reescrito | | |
| `src/ftbase/ingest/select.py` | - | - | nuevo | | |
| `src/ftbase/cli.py` | `pipeline/src/tracklab/cli.py` | `686508a` | reescrito | | |
