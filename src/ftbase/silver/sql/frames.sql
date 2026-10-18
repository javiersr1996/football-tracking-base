-- The physical record of both sports in one shape: one row per match, native
-- frame and entity, in metres, in the venue frame, at the sport's own rate.
--
-- It is built from the tracking and from nothing else. No event coordinate fills
-- a hole here, because task 9 measured that `shots.location` is not the
-- shooter's position at `startFrame` in 10 shots of 1,433, off by as much as 33
-- frames. A frame is what the cameras delivered.
--
-- Four sources, because the two providers put the ball in different places:
-- basketball keeps it as a side of `tracking_objects`, football as columns of
-- `tracking_frames`. Both become the same `<prefix>:ball` entity, and an entity
-- with no observation in a frame produces no row rather than a row of nulls, so
-- that the player engine of task 20 can treat an absence as an absence.
--
-- The boundary rules of the schema, and what each one actually removes:
--
-- * Frames with a null period are excluded. 62,570 football frames carry one
--   (pre-match, half-time and post-match footage in 5 of the 20 matches) and no
--   basketball frame does. The provider already gives no player and no ball on
--   any of them, so the filter removes 0 rows; it stays because the rule is the
--   rule and the next bundle may not be so tidy.
-- * Frames with no object produce no row. In basketball that is about 44% of the
--   file: a timeout keeps its frame in `tracking_frames` with empty lists, and
--   the full timeline lives in bronze.
--
-- z_m follows decision 6. NULL for every player in both sports: basketball sends
-- a constant 0.0, which is not a measurement, and football sends nothing. The
-- ball carries the provider's value converted to metres and uncorrected, with
-- `dim_match.z_ball_status` saying what is known about it.
--
-- is_detected is the column that makes the rest readable. [Lines withdrawn from
-- the public copy of this file: see docs/provenance.md.]
--
-- clock_s is each sport's official clock in its own semantics (decision 3):
-- `gameClock` counts down from 600 s in basketball, and football gets the
-- seconds elapsed since its period's first frame, which is the period half of
-- the match clock task 4 fixed (`timestamp = 45:00 x (period - 1) +
-- (frame - start_frame) / 10`, verified on 100% of non-null timestamps).

{{ config(
    options = {
        'partition_by': 'sport, native_match_id',
        'overwrite': 'true',
    }
) }}

WITH basketball_players AS (
    SELECT
        'basketball' AS sport,
        o.match_id AS native_match_id,
        f.period AS period,
        o.frameIdx AS frame_native,
        f.gameClock AS native_clock_s,
        'bb:' || o.playerId AS entity_id,
        o.x * 0.3048 AS x_m,
        o.y * 0.3048 AS y_m,
        NULL::DOUBLE AS z_m,
        o.isDetected AS is_detected
    FROM {{ source('bronze_basketball', 'tracking_objects') }} o
    JOIN {{ source('bronze_basketball', 'tracking_frames') }} f
        ON f.match_id = o.match_id AND f.frameIdx = o.frameIdx
    WHERE o.side <> 'ball' AND f.period IS NOT NULL
),

basketball_ball AS (
    SELECT
        'basketball',
        o.match_id,
        f.period,
        o.frameIdx,
        f.gameClock,
        'bb:ball',
        o.x * 0.3048,
        o.y * 0.3048,
        o.z * 0.3048,
        o.isDetected
    FROM {{ source('bronze_basketball', 'tracking_objects') }} o
    JOIN {{ source('bronze_basketball', 'tracking_frames') }} f
        ON f.match_id = o.match_id AND f.frameIdx = o.frameIdx
    WHERE o.side = 'ball' AND f.period IS NOT NULL
),

football_players AS (
    SELECT
        'football',
        o.match_id,
        f.period,
        o.frame,
        NULL::DOUBLE,              -- football's clock is derived below, not delivered
        'fb:' || o.player_id,
        o.x,
        o.y,
        NULL::DOUBLE,
        o.is_detected
    FROM {{ source('bronze_football', 'tracking_objects') }} o
    JOIN {{ source('bronze_football', 'tracking_frames') }} f
        ON f.match_id = o.match_id AND f.frame = o.frame
    WHERE f.period IS NOT NULL
),

football_ball AS (
    SELECT
        'football',
        f.match_id,
        f.period,
        f.frame,
        NULL::DOUBLE,
        'fb:ball',
        f.ball_x,
        f.ball_y,
        f.ball_z,
        f.ball_is_detected
    FROM {{ source('bronze_football', 'tracking_frames') }} f
    WHERE f.period IS NOT NULL AND f.ball_x IS NOT NULL
),

tracking AS (
    SELECT * FROM basketball_players
    UNION ALL SELECT * FROM basketball_ball
    UNION ALL SELECT * FROM football_players
    UNION ALL SELECT * FROM football_ball
),

keyed AS (
    SELECT
        t.*,
        s.id_prefix || ':' || t.native_match_id AS match_id,
        s.sample_rate_hz AS sample_rate_hz
    FROM tracking t
    JOIN {{ ref('dim_sport') }} s ON s.sport = t.sport
)

SELECT
    k.sport::VARCHAR AS sport,
    k.match_id::VARCHAR AS match_id,
    k.native_match_id::BIGINT AS native_match_id,
    k.period::SMALLINT AS period,
    k.frame_native::BIGINT AS frame_native,
    (k.frame_native * 1000 / k.sample_rate_hz)::BIGINT AS t_ms,
    coalesce(
        k.native_clock_s,
        (k.frame_native - p.frame_native_start)::DOUBLE / k.sample_rate_hz
    )::DOUBLE AS clock_s,
    k.entity_id::VARCHAR AS entity_id,
    e.entity_kind::VARCHAR AS entity_kind,
    e.team_id::VARCHAR AS team_id,
    k.x_m::DOUBLE AS x_m,
    k.y_m::DOUBLE AS y_m,
    k.z_m::DOUBLE AS z_m,
    k.is_detected::BOOLEAN AS is_detected
FROM keyed k
JOIN {{ ref('dim_period') }} p ON p.match_id = k.match_id AND p.period = k.period
JOIN {{ ref('dim_entity') }} e ON e.match_id = k.match_id AND e.entity_id = k.entity_id
-- Sorted on integer keys only. Within a partition the sport is constant and the
-- period rises with the frame, so this is the order of the naming convention
-- minus the tie-break on entity_id inside a frame, which nothing reads. That
-- tie-break is a VARCHAR sort key over 29 M rows, and with it the sort ran out of
-- memory under a 6 GB limit (the full-build runner's) every time it was tried.
ORDER BY k.native_match_id, k.frame_native
