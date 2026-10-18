-- Everything that can hold a position in a match: the players of both benches
-- and the ball, one row each, per match.
--
-- The roster is the source and not the tracking, on purpose. Every id the
-- tracking uses is in the roster of its match in both sports (checked: 0
-- exceptions), while the roster holds 4 basketball players and 96 football
-- players who never appear in a frame. Building the dimension from the roster
-- therefore covers `frames` without inventing anyone, and a player who sat the
-- whole match is still an entity of that match.
--
-- The ball is a row per match with entity_id '<prefix>:ball', unique inside
-- (match_id, entity_id) as decision 4 asks. It is synthetic in both sports:
-- basketball carries it as a side in tracking_objects and football as columns of
-- tracking_frames, and neither is a roster entry.
--
-- canonical_player_id never rewrites an id: 9 of the 18 alias rows point a
-- duplicated basketball player at another id, and 3 of those 9 land in the
-- rosters of these 10 matches. Applying the alias is gold's job (decision 4); a
-- player with no alias points at themselves, so a consumer can group by the
-- column without a COALESCE of their own.

WITH basketball_players AS (
    SELECT
        'basketball' AS sport,
        g.match_id AS native_match_id,
        g.playerId AS native_entity_id,
        g.teamId AS native_team_id,
        trim(g.firstName || ' ' || g.lastName) AS name,
        g.jersey AS jersey,
        coalesce(a.canonical_player_id, g.playerId) AS native_canonical_player_id
    FROM {{ source('bronze_basketball', 'game_players') }} g
    LEFT JOIN {{ source('bronze_basketball', 'player_id_aliases') }} a ON a.player_id = g.playerId
),

football_players AS (
    SELECT
        'football' AS sport,
        p.match_id AS native_match_id,
        p.id AS native_entity_id,
        p.team_id AS native_team_id,
        trim(p.first_name || ' ' || p.last_name) AS name,
        p.number::VARCHAR AS jersey,
        p.id AS native_canonical_player_id      -- football publishes no alias table
    FROM {{ source('bronze_football', 'match_players') }} p
),

players AS (
    SELECT * FROM basketball_players
    UNION ALL
    SELECT * FROM football_players
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
    FROM players p
    JOIN {{ ref('dim_sport') }} s ON s.sport = p.sport
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
    FROM {{ ref('dim_match') }} m
    JOIN {{ ref('dim_sport') }} s ON s.sport = m.sport
)

SELECT * FROM player_rows
UNION ALL
SELECT * FROM ball_rows
