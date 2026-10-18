-- Rewritten from tracklab@686508a: pipeline/dbt/models/silver/dim_team.sql
-- Changes: football only (the basketball branch and the union dropped); dbt source() and
-- ref() replaced by the table names ftbase.silver.build registers.
--
-- One row per team seen in the bundle, 13 of them, with the sport's prefix on the
-- id (`fb:<id>`), the key every other silver table uses.
--
-- The name is read once per team and not per match: the bundle is stable, 0 teams
-- carry two different names across their matches (checked in tracklab before
-- writing this model, and the unique check on team_id keeps it checked).

WITH football AS (
    SELECT home_team_id AS native_team_id, home_team_name AS name, home_team_short_name AS short_name
    FROM bronze_match
    UNION ALL
    SELECT away_team_id, away_team_name, away_team_short_name
    FROM bronze_match
),

teams AS (
    SELECT 'football' AS sport, native_team_id, max(name) AS name, max(short_name) AS short_name
    FROM football
    GROUP BY ALL
)

SELECT
    (s.id_prefix || ':' || t.native_team_id)::VARCHAR AS team_id,
    t.sport::VARCHAR AS sport,
    t.native_team_id::BIGINT AS native_team_id,
    trim(t.name)::VARCHAR AS name,
    trim(t.short_name)::VARCHAR AS short_name
FROM teams t
JOIN dim_sport s ON s.sport = t.sport
