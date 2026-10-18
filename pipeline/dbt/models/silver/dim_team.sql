-- One row per team seen in the bundles, with the sport's prefix on the id
-- because SkillCorner does not promise that its basketball and football id
-- spaces never collide (decision 4). 17 basketball teams, 13 football.
--
-- The name is read once per team and not per match: both bundles are stable,
-- 0 teams carry two different names across their matches (checked before
-- writing this model, and the `unique` test on team_id keeps it checked).
-- Basketball publishes no short name, so short_name is NULL there rather than a
-- copy of `name` that would look like a measurement.

WITH basketball AS (
    SELECT home_team_id AS native_team_id, home_team_name AS name
    FROM {{ source('bronze_basketball', 'matches') }}
    UNION ALL
    SELECT away_team_id, away_team_name
    FROM {{ source('bronze_basketball', 'matches') }}
),

football AS (
    SELECT home_team_id AS native_team_id, home_team_name AS name, home_team_short_name AS short_name
    FROM {{ source('bronze_football', 'match') }}
    UNION ALL
    SELECT away_team_id, away_team_name, away_team_short_name
    FROM {{ source('bronze_football', 'match') }}
),

teams AS (
    SELECT 'basketball' AS sport, native_team_id, max(name) AS name, NULL AS short_name
    FROM basketball
    GROUP BY ALL
    UNION ALL
    SELECT 'football', native_team_id, max(name), max(short_name)
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
JOIN {{ ref('dim_sport') }} s ON s.sport = t.sport
