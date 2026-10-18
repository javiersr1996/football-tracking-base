-- Rewritten from tracklab@686508a: pipeline/dbt/models/silver/events.sql
-- Changes: football only (the 20 basketball families, their Jinja loop and the union
-- dropped); no dbt config block: ftbase.silver.build runs this once per match with the
-- bronze event views pointed at that match and writes one Parquet file per match; the
-- Jinja `side` macro written out as its CASE; dbt source() and ref() replaced by table
-- names.
--
-- Every event of the bundle in one shape: the 4 native families, the phases of
-- play, and the team possession this model derives because the provider does not
-- deliver one.
--
-- The table keeps only what is filled with the same meaning across families.
-- Everything else stays in bronze and comes back with a join on
-- (match_id, native_family, native_event_id), which is why those three columns
-- travel on every row. The EPV of an event is one join away and is not copied here.
--
-- **Coordinates are rotated to the venue frame.** The provider delivers events in
-- an attack frame (the team in possession attacks towards +x) and silver keeps the
-- venue frame of the tracking, so each row is turned by its own `attacking_side`.
-- A row whose side is neither `left_to_right` nor `right_to_left` gets NULL
-- coordinates rather than a default sign.
--
-- **The team possession is read, not guessed.** Two candidate rules
-- were considered. Merging consecutive phases of the same team makes possessions
-- that overlap their neighbour. Following the provider's own
-- `first_player_possession_in_team_possession` flag (and opening one more when a
-- new period starts, below) overlaps none. The chain rule wins: it never overlaps,
-- and it is the provider delimiting its own team possessions rather than
-- this project inferring them from phases that the file itself says can contain a
-- loss.

-- The team possession: open one on each player_possession the provider flags as
-- the first of a team possession, close it on the row before the next. A new
-- period opens one too: the provider does not flag the first touch after the break
-- when the same team still has the ball, and a chain across the break would carry
-- one period and one attacking side for both halves.
WITH football_touches AS (
    SELECT
        match_id, period, team_id, player_id, event_id,
        frame_start, frame_end, x_start, y_start, x_end, y_end, attacking_side, end_type,
        first_player_possession_in_team_possession
            OR coalesce(period <> lag(period) OVER w, false) AS opens,
        row_number() OVER w AS rn
    FROM bronze_dynamic_events
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
    FROM bronze_dynamic_events e

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
    FROM bronze_phases_of_play p

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
        CASE f.attacking_side WHEN 'left_to_right' THEN 1 WHEN 'right_to_left' THEN -1 END
            * f.x_start_native AS x_start_m,
        CASE f.attacking_side WHEN 'left_to_right' THEN 1 WHEN 'right_to_left' THEN -1 END
            * f.y_start_native AS y_start_m,
        CASE f.attacking_side WHEN 'left_to_right' THEN 1 WHEN 'right_to_left' THEN -1 END
            * f.x_end_native AS x_end_m,
        CASE f.attacking_side WHEN 'left_to_right' THEN 1 WHEN 'right_to_left' THEN -1 END
            * f.y_end_native AS y_end_m,
        f.native_subtype, f.outcome
    FROM football_raw f
    LEFT JOIN football_possessions p
        ON p.native_match_id = f.native_match_id
        AND f.frame_start BETWEEN p.frame_start AND p.frame_end
),

typed AS (
    -- The conformed vocabulary. A mapping with a literal subtype wins over the
    -- catch-all of its family, and anything the seed does not name is `other`,
    -- told apart by native_family.
    SELECT
        u.*,
        m.event_type AS mapped_event_type
    FROM football u
    LEFT JOIN event_type_map m
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
JOIN dim_sport s ON s.sport = t.sport
ORDER BY t.sport, t.native_match_id, t.period, t.frame_start, t.event_id
