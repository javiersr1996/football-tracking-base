-- Rewritten from tracklab@686508a: pipeline/dbt/models/silver/dim_entity.sql
-- Changes: football only (the basketball players, their alias table and the union
-- dropped); dbt source() and ref() replaced by table names.
--
-- Everything that can hold a position in a match: the players of both benches
-- and the ball, one row each, per match.
--
-- The roster is the source and not the tracking, on purpose. Every id the
-- tracking uses is in the roster of its match, while the roster also holds players who
-- never appear in a frame. Building the dimension from the roster therefore covers `frames` without
-- inventing anyone, and a player who sat the whole match is still an entity of
-- that match.
--
-- The ball is a row per match with entity_id 'fb:ball', unique inside
-- (match_id, entity_id). It is synthetic: football carries it as columns of
-- tracking_frames, not as a roster entry.
--
-- canonical_player_id never rewrites an id: football publishes no alias table,
-- so every player points at themselves, and a consumer can group by the column
-- without a COALESCE of their own.

WITH football_players AS (
    SELECT
        'football' AS sport,
        p.match_id AS native_match_id,
        p.id AS native_entity_id,
        p.team_id AS native_team_id,
        trim(p.first_name || ' ' || p.last_name) AS name,
        p.number::VARCHAR AS jersey,
        p.id AS native_canonical_player_id      -- football publishes no alias table
    FROM bronze_match_players p
),

player_rows AS (
    SELECT
        (s.id_prefix || ':' || p.native_match_id)::VARCHAR AS match_id,
        (s.id_prefix || ':' || p.native_entity_id)::VARCHAR AS entity_id,
        'player'::VARCHAR AS entity_kind,
        (s.id_prefix || ':' || p.native_team_id)::VARCHAR AS team_id,
        p.native_entity_id::BIGINT AS native_entity_id,
        p.name::VARCHAR AS name,
        p.jersey::VARCHAR AS jersey,
        (s.id_prefix || ':' || p.native_canonical_player_id)::VARCHAR AS canonical_player_id
    FROM football_players p
    JOIN dim_sport s ON s.sport = p.sport
),

ball_rows AS (
    SELECT
        m.match_id,
        (s.id_prefix || ':ball')::VARCHAR AS entity_id,
        'ball'::VARCHAR AS entity_kind,
        NULL::VARCHAR AS team_id,
        NULL::BIGINT AS native_entity_id,
        'ball'::VARCHAR AS name,
        NULL::VARCHAR AS jersey,
        NULL::VARCHAR AS canonical_player_id
    FROM dim_match m
    JOIN dim_sport s ON s.sport = m.sport
)

SELECT * FROM player_rows
UNION ALL
SELECT * FROM ball_rows
