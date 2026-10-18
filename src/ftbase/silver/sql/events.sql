-- Every event of both bundles in one shape: 20 native families in basketball,
-- 4 in football plus its phases of play, and the football team possession this
-- model derives because the provider does not deliver one.
--
-- The table keeps only what both sports fill with the same meaning (decision 5).
-- Everything else stays in bronze and comes back with a join on
-- (match_id, native_family, native_event_id), which is why those three columns
-- travel on every row. The EPV of a football event and the shot quality of a
-- basketball one are one join away and are not copied here.
--
-- **Coordinates are rotated, and a row with no rotation gets no coordinates.**
-- Both providers deliver events in an attack frame and silver keeps the venue
-- frame (decision 2). Basketball rotates by the `leftHoop` of the event's
-- possession, football by its own `attacking_side`. A basketball row whose
-- possession does not resolve has no rotation, so its coordinates are NULL
-- rather than rotated by a default sign: that exact bug was found in task 9's
-- probe by a synthetic test, because no row in these 10 matches triggers it.
--
-- **What a shot's coordinates are, and are not.** `shots.location` is where the
-- provider says the shot happened. Task 9 measured that it is not the shooter's
-- position at `startFrame` in 10 shots of 1,433, off by up to 33 frames, so
-- these columns are never joined to `frames` to infer where a player stood.
--
-- **The football team possession is read, not guessed.** `tracklab possessions`
-- scored the plan's two candidate rules against a third source neither uses,
-- the per-frame `possession_group` of the tracking: merging consecutive phases
-- of the same team gives 5,092 possessions covering 96.67% of the frames the
-- provider itself attributes to a team, but 3,030 of them overlap their
-- neighbour and they agree with the tracking on the team in 76.87% of the
-- frames of the worst match. Following the provider's own
-- `first_player_possession_in_team_possession` flag gives 3,998 (4,009 once a
-- new period opens one too, below), covering
-- 85.82%, overlapping none and agreeing 79.78% in the worst match. The chain
-- rule wins: it never overlaps, it agrees more, and it is the provider
-- delimiting its own team possessions rather than this project inferring them
-- from phases that the file itself says can contain a loss.

{{ config(
    options = {
        'partition_by': 'sport, native_match_id',
        'overwrite': 'true',
    }
) }}

{#
  One entry per basketball family: where its frames, its team, its players, its
  coordinates, its subtype and its outcome live. Written out in full so the 20
  mappings can be read side by side instead of hidden in 20 near-identical
  blocks. Every coordinate is in feet as delivered, in the attack frame.
#}
{% set BASKETBALL = [
  {'family': 'possessions',      'possession': 'id',           'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'NULL',           'p2': 'NULL',              'xs': 'NULL',        'ys': 'NULL',        'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'startType',   'outcome': 'outcome'},
  {'family': 'chances',          'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'NULL',           'p2': 'NULL',              'xs': 'NULL',        'ys': 'NULL',        'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'startType',   'outcome': 'outcome'},
  {'family': 'chance_players',   'possession': 'possessionId', 'fs': 'NULL',       'fe': 'NULL',       'team': 'NULL',         'p1': 'playerId',       'p2': 'NULL',              'xs': 'shotLoc_x',   'ys': 'shotLoc_y',   'xe': 'reboundLoc_x',  'ye': 'reboundLoc_y',  'subtype': 'NULL',        'outcome': 'NULL'},
  {'family': 'closeouts',        'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'ballhandlerId',  'p2': 'ballhandlerDefId',  'xs': 'startLoc_x',  'ys': 'startLoc_y',  'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'bhrAction',   'outcome': "array_to_string(bhrOutcomes, ';')"},
  {'family': 'dribbles',         'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'offTeamId',    'p1': 'ballhandlerId',  'p2': 'NULL',              'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'region',      'outcome': 'NULL'},
  {'family': 'drives',           'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'ballhandlerId',  'p2': 'ballhandlerDefId',  'xs': 'location_x',  'ys': 'location_y',  'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'category',    'outcome': 'endType'},
  {'family': 'fouls',            'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'foulerTeamId', 'p1': 'foulerId',       'p2': 'fouledId',          'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'foulType',    'outcome': 'NULL'},
  {'family': 'free_throws',      'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'offTeamId',    'p1': 'shooterId',      'p2': 'NULL',              'xs': 'NULL',        'ys': 'NULL',        'xe': 'NULL',          'ye': 'NULL',          'subtype': 'NULL',        'outcome': "CASE WHEN outcome THEN 'made' ELSE 'missed' END"},
  {'family': 'handoffs',         'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'offTeamId',    'p1': 'setterId',       'p2': 'receiverId',        'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'locationType','outcome': "array_to_string(bhrOutcomes, ';')"},
  {'family': 'isolations',       'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'ballhandlerId',  'p2': 'ballhandlerDefId',  'xs': 'location_x',  'ys': 'location_y',  'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'NULL',        'outcome': "array_to_string(bhrOutcomes, ';')"},
  {'family': 'matchups',         'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'NULL',         'p1': 'offPlayerId',    'p2': 'defPlayerId',       'xs': 'NULL',        'ys': 'NULL',        'xe': 'NULL',          'ye': 'NULL',          'subtype': 'NULL',        'outcome': 'NULL'},
  {'family': 'off_ball_screens', 'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'offTeamId',    'p1': 'cutterId',       'p2': 'screenerId',        'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'NULL',        'outcome': "array_to_string(outcomes, ';')"},
  {'family': 'passes',           'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'passerId',       'p2': 'receiverId',        'xs': 'passerLoc_x', 'ys': 'passerLoc_y', 'xe': 'receiverLoc_x', 'ye': 'receiverLoc_y', 'subtype': 'NULL',        'outcome': "CASE WHEN complete THEN 'complete' WHEN turnover THEN 'turnover' ELSE 'incomplete' END"},
  {'family': 'picks',            'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'offTeamId',    'p1': 'ballhandlerId',  'p2': 'screenerId',        'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'locationType','outcome': "array_to_string(bhrOutcomes, ';')"},
  {'family': 'posts',            'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'ballhandlerId',  'p2': 'ballhandlerDefId',  'xs': 'location_x',  'ys': 'location_y',  'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'section',     'outcome': "array_to_string(bhrOutcomes, ';')"},
  {'family': 'rebounds',         'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'teamId',       'p1': 'rebounderId',    'p2': 'NULL',              'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': "CASE WHEN defensive THEN 'defensive' ELSE 'offensive' END", 'outcome': 'NULL'},
  {'family': 'shots',            'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'shooterId',      'p2': 'passerId',          'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'shotType',    'outcome': "CASE WHEN outcome THEN 'made' ELSE 'missed' END"},
  {'family': 'timeouts',         'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'teamId',       'p1': 'NULL',           'p2': 'NULL',              'xs': 'NULL',        'ys': 'NULL',        'xe': 'NULL',          'ye': 'NULL',          'subtype': 'NULL',        'outcome': 'NULL'},
  {'family': 'touches',          'possession': 'possessionId', 'fs': 'startFrame', 'fe': 'endFrame',   'team': 'offTeamId',    'p1': 'playerId',       'p2': 'defenderId',        'xs': 'location_x',  'ys': 'location_y',  'xe': 'endLoc_x',      'ye': 'endLoc_y',      'subtype': 'NULL',        'outcome': "array_to_string(outcomes, ';')"},
  {'family': 'turnovers',        'possession': 'possessionId', 'fs': 'frame',      'fe': 'frame',      'team': 'offTeamId',    'p1': 'turnedOverId',   'p2': 'stealerId',         'xs': 'location_x',  'ys': 'location_y',  'xe': 'NULL',          'ye': 'NULL',          'subtype': 'NULL',        'outcome': 'NULL'},
] %}

WITH basketball_rotation AS (
    -- (x, y) when leftHoop, (-x, -y) when not, NULL when the possession is unknown.
    SELECT id AS possession_native_id, match_id,
           CASE WHEN leftHoop THEN 1 WHEN NOT leftHoop THEN -1 END AS venue_factor
    FROM {{ source('bronze_basketball', 'events_possessions') }}
),

basketball_raw AS (
{% for f in BASKETBALL %}
    SELECT
        'basketball' AS sport,
        e.match_id AS native_match_id,
        '{{ f.family }}' AS native_family,
        e.id::VARCHAR AS native_event_id,
        e.{{ f.possession }}::VARCHAR AS possession_native_id,
        e.period AS period,
        {{ f.fs }}::BIGINT AS frame_start,
        {{ f.fe }}::BIGINT AS frame_end,
        {{ f.team }}::BIGINT AS native_team_id,
        {{ f.p1 }}::BIGINT AS native_player_id,
        {{ f.p2 }}::BIGINT AS native_player2_id,
        {{ f.xs }}::DOUBLE AS x_start_native,
        {{ f.ys }}::DOUBLE AS y_start_native,
        {{ f.xe }}::DOUBLE AS x_end_native,
        {{ f.ye }}::DOUBLE AS y_end_native,
        {{ f.subtype }}::VARCHAR AS native_subtype,
        {{ f.outcome }}::VARCHAR AS outcome
    FROM {{ source('bronze_basketball', 'events_' ~ f.family) }} e
{% if not loop.last %}    UNION ALL{% endif %}
{% endfor %}
),

basketball AS (
    SELECT
        b.sport, b.native_match_id, b.native_family, b.native_event_id,
        'bb:' || b.native_family || ':' || b.native_event_id AS event_id,
        'bb:possessions:' || b.possession_native_id AS possession_id,
        b.period, b.frame_start, b.frame_end,
        'bb:' || b.native_team_id AS team_id,
        'bb:' || b.native_player_id AS player_id,
        'bb:' || b.native_player2_id AS player2_id,
        r.venue_factor * b.x_start_native * 0.3048 AS x_start_m,
        r.venue_factor * b.y_start_native * 0.3048 AS y_start_m,
        r.venue_factor * b.x_end_native * 0.3048 AS x_end_m,
        r.venue_factor * b.y_end_native * 0.3048 AS y_end_m,
        b.native_subtype, b.outcome
    FROM basketball_raw b
    LEFT JOIN basketball_rotation r
        ON r.match_id = b.native_match_id AND r.possession_native_id = b.possession_native_id
),

-- Football's team possession: open one on each player_possession the provider
-- flags as the first of a team possession, close it on the row before the next.
-- A new period opens one too: the provider does not flag the first touch after the
-- break when the same team still has the ball, and a chain across the break would
-- carry one period and one attacking side for both halves.
football_touches AS (
    SELECT
        match_id, period, team_id, player_id, event_id,
        frame_start, frame_end, x_start, y_start, x_end, y_end, attacking_side, end_type,
        first_player_possession_in_team_possession
            OR coalesce(period <> lag(period) OVER w, false) AS opens,
        row_number() OVER w AS rn
    FROM {{ source('bronze_football', 'dynamic_events') }}
    WHERE event_type = 'player_possession'
    WINDOW w AS (PARTITION BY match_id ORDER BY frame_start, event_id)
),

football_chains AS (
    SELECT *, sum(CASE WHEN opens THEN 1 ELSE 0 END) OVER (
        PARTITION BY match_id ORDER BY rn
    ) AS chain
    FROM football_touches
),

football_possessions AS (
    SELECT
        match_id AS native_match_id,
        chain,
        'fb:possession:' || match_id || ':' || chain AS event_id,
        any_value(period) AS period,
        min(frame_start) AS frame_start,
        max(frame_end) AS frame_end,
        any_value(team_id) AS native_team_id,
        min_by(x_start, rn) AS x_start_native,
        min_by(y_start, rn) AS y_start_native,
        max_by(x_end, rn) AS x_end_native,
        max_by(y_end, rn) AS y_end_native,
        any_value(attacking_side) AS attacking_side,
        max_by(end_type, rn) AS outcome
    FROM football_chains
    WHERE chain > 0
    GROUP BY match_id, chain
),

football_raw AS (
    SELECT
        e.match_id AS native_match_id,
        e.event_type AS native_family,
        e.event_id::VARCHAR AS native_event_id,
        'fb:' || e.event_type || ':' || e.match_id || ':' || e.event_id AS event_id,
        e.period, e.frame_start, e.frame_end,
        e.team_id AS native_team_id,
        e.player_id AS native_player_id,
        e.player_in_possession_id AS native_player2_id,
        e.x_start AS x_start_native, e.y_start AS y_start_native,
        e.x_end AS x_end_native, e.y_end AS y_end_native,
        e.attacking_side,
        -- A player_possession carries no event_subtype: how it ended is the only
        -- classification the provider gives it, so it is both subtype and outcome.
        CASE WHEN e.event_type = 'player_possession' THEN e.end_type ELSE e.event_subtype END
            AS native_subtype,
        CASE WHEN e.event_type = 'player_possession' THEN e.end_type END AS outcome
    FROM {{ source('bronze_football', 'dynamic_events') }} e

    UNION ALL

    SELECT
        p.match_id, 'phases_of_play', p.index::VARCHAR,
        'fb:phases_of_play:' || p.match_id || ':' || p.index,
        p.period, p.frame_start, p.frame_end,
        p.team_in_possession_id, NULL, NULL,
        p.x_start, p.y_start, p.x_end, p.y_end,
        p.attacking_side,
        p.team_in_possession_phase_type,
        CASE
            WHEN p.team_possession_lead_to_goal THEN 'goal'
            WHEN p.team_possession_lead_to_shot THEN 'shot'
            WHEN p.team_possession_loss_in_phase THEN 'loss'
        END
    FROM {{ source('bronze_football', 'phases_of_play') }} p

    UNION ALL

    SELECT
        q.native_match_id, 'possession', NULL, q.event_id,
        q.period, q.frame_start, q.frame_end,
        q.native_team_id, NULL, NULL,
        q.x_start_native, q.y_start_native, q.x_end_native, q.y_end_native,
        q.attacking_side, NULL, q.outcome
    FROM football_possessions q
),

football AS (
    SELECT
        'football' AS sport,
        f.native_match_id, f.native_family, f.native_event_id, f.event_id,
        p.event_id AS possession_id,
        f.period, f.frame_start, f.frame_end,
        'fb:' || f.native_team_id AS team_id,
        'fb:' || f.native_player_id AS player_id,
        'fb:' || f.native_player2_id AS player2_id,
        {% set side = "CASE f.attacking_side WHEN 'left_to_right' THEN 1 WHEN 'right_to_left' THEN -1 END" %}
        {{ side }} * f.x_start_native AS x_start_m,
        {{ side }} * f.y_start_native AS y_start_m,
        {{ side }} * f.x_end_native AS x_end_m,
        {{ side }} * f.y_end_native AS y_end_m,
        f.native_subtype, f.outcome
    FROM football_raw f
    LEFT JOIN football_possessions p
        ON p.native_match_id = f.native_match_id
        AND f.frame_start BETWEEN p.frame_start AND p.frame_end
),

united AS (
    SELECT * FROM basketball
    UNION ALL
    SELECT * FROM football
),

typed AS (
    -- The conformed vocabulary. A mapping with a literal subtype wins over the
    -- catch-all of its family, and anything the seed does not name is `other`,
    -- told apart by native_family.
    SELECT
        u.*,
        m.event_type AS mapped_event_type
    FROM united u
    LEFT JOIN {{ ref('event_type_map') }} m
        ON m.sport = u.sport
        AND m.native_family = u.native_family
        AND (m.native_subtype_pattern = '*' OR m.native_subtype_pattern = u.native_subtype)
    QUALIFY row_number() OVER (
        PARTITION BY u.sport, u.native_match_id, u.event_id
        ORDER BY CASE WHEN m.native_subtype_pattern = '*' THEN 1 ELSE 0 END
    ) = 1
)

SELECT
    t.sport::VARCHAR AS sport,
    (s.id_prefix || ':' || t.native_match_id)::VARCHAR AS match_id,
    t.native_match_id::BIGINT AS native_match_id,
    t.event_id::VARCHAR AS event_id,
    coalesce(t.mapped_event_type, 'other')::VARCHAR AS event_type,
    t.native_family::VARCHAR AS native_family,
    t.native_event_id::VARCHAR AS native_event_id,
    t.native_subtype::VARCHAR AS native_subtype,
    t.period::SMALLINT AS period,
    t.frame_start::BIGINT AS frame_start,
    t.frame_end::BIGINT AS frame_end,
    (t.frame_start * 1000 / s.sample_rate_hz)::BIGINT AS t_ms_start,
    (t.frame_end * 1000 / s.sample_rate_hz)::BIGINT AS t_ms_end,
    t.team_id::VARCHAR AS team_id,
    t.player_id::VARCHAR AS player_id,
    t.player2_id::VARCHAR AS player2_id,
    t.x_start_m::DOUBLE AS x_start_m,
    t.y_start_m::DOUBLE AS y_start_m,
    t.x_end_m::DOUBLE AS x_end_m,
    t.y_end_m::DOUBLE AS y_end_m,
    t.possession_id::VARCHAR AS possession_id,
    t.outcome::VARCHAR AS outcome
FROM typed t
JOIN {{ ref('dim_sport') }} s ON s.sport = t.sport
ORDER BY t.sport, t.native_match_id, t.period, t.frame_start, t.event_id
