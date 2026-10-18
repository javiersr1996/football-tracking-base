-- One row per match and period, with the native frame it opens and closes at and
-- the same instant in t_ms, so a reader never has to know a sport's frame rate
-- to place a period on the video clock.
--
-- Where the boundaries come from is not the same in the two bundles, and that is
-- the whole point of the model. Football declares them: match.json carries
-- `match_periods` with start_frame and end_frame. Basketball declares nothing,
-- so they are read off the tracking, which is the only place the period changes
-- hands. Frames of a timeout keep their period and stay inside the range; frames
-- before the tip-off carry a null period and are excluded here and in `frames`.
--
-- regulation_s is the length the competition gives the period, not the length it
-- lasted: 600 s for an ACB quarter, 300 s from the fifth period on (overtime,
-- and gameClock tops out at 298 s in the one match that has one), 2700 s for a
-- football half. It is what a clock needs to be drawn, and never a duration.

WITH basketball AS (
    SELECT
        'basketball' AS sport,
        match_id AS native_match_id,
        period,
        min(frameIdx) AS frame_native_start,
        max(frameIdx) AS frame_native_end
    FROM {{ source('bronze_basketball', 'tracking_frames') }}
    WHERE period IS NOT NULL
    GROUP BY ALL
),

football AS (
    SELECT
        'football' AS sport,
        match_id AS native_match_id,
        period,
        start_frame AS frame_native_start,
        end_frame AS frame_native_end
    FROM {{ source('bronze_football', 'match_periods') }}
),

periods AS (
    SELECT * FROM basketball
    UNION ALL
    SELECT * FROM football
)

SELECT
    (s.id_prefix || ':' || p.native_match_id)::VARCHAR AS match_id,
    p.period::SMALLINT AS period,
    p.frame_native_start::BIGINT AS frame_native_start,
    p.frame_native_end::BIGINT AS frame_native_end,
    (p.frame_native_start * 1000 / s.sample_rate_hz)::BIGINT AS t_ms_start,
    (p.frame_native_end * 1000 / s.sample_rate_hz)::BIGINT AS t_ms_end,
    CASE
        WHEN p.sport = 'football' THEN 2700
        WHEN p.period <= 4 THEN 600
        ELSE 300
    END::INTEGER AS regulation_s
FROM periods p
JOIN {{ ref('dim_sport') }} s ON s.sport = p.sport
