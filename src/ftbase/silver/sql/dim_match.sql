-- Rewritten from tracklab@686508a: pipeline/dbt/models/silver/dim_match.sql
-- Changes: football only (the basketball branch, its court report and the surface_spec
-- seed dropped); the pinned commit comes from `manifest_sources`, which
-- ftbase.silver.build fills from the manifest, instead of a dbt source over the JSON;
-- dbt source() and ref() replaced by table names.
--
-- One row per match, and the place a reader goes to find out what a
-- match is made of before touching the frames.
--
-- Three columns here are conclusions rather than copies:
--
-- * surface_*. Football declares its pitch and it is not the same in every
--   match, so it is read per match from match.json.
-- * z_ball_status. 'unverified': the provider delivers ball_z whenever the ball
--   is present and nothing here has checked it against physics. Silver corrects
--   nothing either way.
-- * z_player_status. 'absent': football sends no z for players at all, so it is
--   NULL in `frames`.
--
-- n_periods is counted from dim_period rather than declared, so the two models
-- cannot disagree about how many periods a match had.

WITH commits AS (
    SELECT source_id::VARCHAR AS source_id, source_commit::VARCHAR AS source_commit
    FROM manifest_sources
),

football AS (
    SELECT
        'football' AS sport,
        f.id AS native_match_id,
        f.competition_edition_competition_name AS competition,
        f.competition_edition_season_name AS season,
        f.date_time AS date_time,
        f.home_team_id AS home_team_id,
        f.away_team_id AS away_team_id,
        f.home_team_score AS home_score,
        f.away_team_score AS away_score,
        'provider_' || f.pitch_length || 'x' || f.pitch_width AS surface_spec,
        f.pitch_length AS surface_length_m,
        f.pitch_width AS surface_width_m
    FROM bronze_match f
),

periods AS (
    SELECT match_id, count(*) AS n_periods
    FROM dim_period
    GROUP BY ALL
)

SELECT
    (s.id_prefix || ':' || m.native_match_id)::VARCHAR AS match_id,
    m.sport::VARCHAR AS sport,
    m.native_match_id::BIGINT AS native_match_id,
    m.competition::VARCHAR AS competition,
    m.season::VARCHAR AS season,
    strptime(m.date_time, '%Y-%m-%dT%H:%M:%SZ')::TIMESTAMP AS kickoff_utc,
    (s.id_prefix || ':' || m.home_team_id)::VARCHAR AS home_team_id,
    (s.id_prefix || ':' || m.away_team_id)::VARCHAR AS away_team_id,
    m.home_score::SMALLINT AS home_score,
    m.away_score::SMALLINT AS away_score,
    m.surface_length_m::DOUBLE AS surface_length_m,
    m.surface_width_m::DOUBLE AS surface_width_m,
    m.surface_spec::VARCHAR AS surface_spec,
    p.n_periods::SMALLINT AS n_periods,
    'unverified'::VARCHAR AS z_ball_status,
    'absent'::VARCHAR AS z_player_status,
    c.source_commit::VARCHAR AS source_commit
FROM football m
JOIN dim_sport s ON s.sport = m.sport
JOIN periods p ON p.match_id = s.id_prefix || ':' || m.native_match_id
-- The sport's own bundle: the manifest may list other sources whose commit is not
-- the one the match rows came from.
JOIN commits c ON c.source_id = 'opendata-' || m.sport
