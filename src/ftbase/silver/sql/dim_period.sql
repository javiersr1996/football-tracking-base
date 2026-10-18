-- Rewritten from tracklab@686508a: pipeline/dbt/models/silver/dim_period.sql
-- Changes: football only (the basketball branch and the union dropped); dbt source() and
-- ref() replaced by the table names ftbase.silver.build registers.
--
-- One row per match and period, with the native frame it opens and closes at and
-- the same instant in t_ms, so a reader never has to know the frame rate to place
-- a period on the video clock.
--
-- Football declares the boundaries: match.json carries `match_periods` with
-- start_frame and end_frame.
--
-- regulation_s is the length the competition gives the period, not the length it
-- lasted: 2700 s for a football half. It is what a clock needs to be drawn, and
-- never a duration.

WITH periods AS (
    SELECT
        'football' AS sport,
        match_id AS native_match_id,
        period,
        start_frame AS frame_native_start,
        end_frame AS frame_native_end
    FROM bronze_match_periods
)

SELECT
    (s.id_prefix || ':' || p.native_match_id)::VARCHAR AS match_id,
    p.period::SMALLINT AS period,
    p.frame_native_start::BIGINT AS frame_native_start,
    p.frame_native_end::BIGINT AS frame_native_end,
    (p.frame_native_start * 1000 / s.sample_rate_hz)::BIGINT AS t_ms_start,
    (p.frame_native_end * 1000 / s.sample_rate_hz)::BIGINT AS t_ms_end,
    2700::INTEGER AS regulation_s
FROM periods p
JOIN dim_sport s ON s.sport = p.sport
