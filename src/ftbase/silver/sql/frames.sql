-- Rewritten from tracklab@686508a: pipeline/dbt/models/silver/frames.sql
-- Changes: football only (the two basketball sources and the figures that mixed both
-- sports dropped); no dbt config block: ftbase.silver.build runs this once per match
-- with the bronze tracking views pointed at that match and writes one Parquet file per
-- match; dbt source() and ref() replaced by table names.
--
-- The physical record in one shape: one row per match, native frame and entity,
-- in metres, in the venue frame, at 10 Hz.
--
-- It is built from the tracking and from nothing else. No event coordinate fills
-- a hole here: a frame is what the cameras delivered.
--
-- Two sources, because the provider puts the ball in a different place from the
-- players: players are rows of `tracking_objects`, the ball is columns of
-- `tracking_frames`. The ball becomes the `fb:ball` entity, and an entity with no
-- observation in a frame produces no row rather than a row of nulls, so that an
-- absence stays an absence.
--
-- The boundary rule of the schema, and what it actually removes: frames with a
-- null period are excluded. 62,570 frames carry one (pre-match, half-time and
-- post-match footage in 5 of the 20 matches). The provider already gives no
-- player and no ball on any of them, so the filter removes 0 rows; it stays
-- because the rule is the rule and the next bundle may not be so tidy.
--
-- z_m is NULL for every player, because football sends nothing. The ball carries
-- the provider's value, uncorrected, with `dim_match.z_ball_status` saying what is
-- known about it.
--
-- is_detected is the provider's own flag, kept as delivered for every player and
-- for the ball. Nothing here reads it, filters on it or interprets it.
--
-- clock_s is the official clock in its own semantics: the seconds elapsed since
-- the period's first frame, which is the period half of the match clock
-- (`timestamp = 45:00 x (period - 1) + (frame - start_frame) / 10`, verified in
-- tracklab on 100% of non-null timestamps).

WITH football_players AS (
    SELECT
        'football' AS sport,
        o.match_id AS native_match_id,
        f.period AS period,
        o.frame AS frame_native,
        NULL::DOUBLE AS native_clock_s,  -- football's clock is derived below, not delivered
        'fb:' || o.player_id AS entity_id,
        o.x AS x_m,
        o.y AS y_m,
        NULL::DOUBLE AS z_m,
        o.is_detected AS is_detected
    FROM bronze_tracking_objects o
    JOIN bronze_tracking_frames f
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
    FROM bronze_tracking_frames f
    WHERE f.period IS NOT NULL AND f.ball_x IS NOT NULL
),

tracking AS (
    SELECT * FROM football_players
    UNION ALL SELECT * FROM football_ball
),

keyed AS (
    SELECT
        t.*,
        s.id_prefix || ':' || t.native_match_id AS match_id,
        s.sample_rate_hz AS sample_rate_hz
    FROM tracking t
    JOIN dim_sport s ON s.sport = t.sport
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
JOIN dim_period p ON p.match_id = k.match_id AND p.period = k.period
JOIN dim_entity e ON e.match_id = k.match_id AND e.entity_id = k.entity_id
-- Sorted on integer keys only. The period rises with the frame, so this is the
-- order of the naming convention minus the tie-break on entity_id inside a frame,
-- which nothing reads. That tie-break is a VARCHAR sort key, and with it the sort
-- of the whole table ran out of memory under a 6 GB limit in tracklab.
ORDER BY k.native_match_id, k.frame_native
