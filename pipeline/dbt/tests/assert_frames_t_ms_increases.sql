-- t_ms is the clock that joins frames to events and drives the player of task
-- 20, so it has to move forward for a given entity inside a period. An equal or
-- smaller t_ms than the previous row would mean a duplicated frame or a frame
-- counter that went backwards, and the interpolation would silently divide by a
-- non-positive interval.

SELECT match_id, period, entity_id, t_ms, previous_t_ms
FROM (
    SELECT
        match_id,
        period,
        entity_id,
        t_ms,
        lag(t_ms) OVER (PARTITION BY match_id, period, entity_id ORDER BY t_ms) AS previous_t_ms
    FROM {{ ref('frames') }}
)
WHERE previous_t_ms IS NOT NULL AND t_ms <= previous_t_ms
