-- Copied from tracklab@686508a: pipeline/dbt/tests/assert_frames_t_ms_increases.sql
-- Changes: dbt ref() replaced by the table name; a player id named in a comment dropped;
-- run by ftbase.silver.checks.
--
-- t_ms is the clock that joins frames to events, so it has to move forward for a
-- given entity inside a period. An equal or smaller t_ms than the previous row
-- would mean a duplicated frame or a frame counter that went backwards.

SELECT match_id, period, entity_id, t_ms, previous_t_ms
FROM (
    SELECT
        match_id,
        period,
        entity_id,
        t_ms,
        lag(t_ms) OVER (PARTITION BY match_id, period, entity_id ORDER BY t_ms) AS previous_t_ms
    FROM frames
)
WHERE previous_t_ms IS NOT NULL AND t_ms <= previous_t_ms
