-- The invariant the chain rule was chosen for. Football's team possessions are
-- derived here, so nothing outside this project guarantees they tile the match
-- without stepping on each other; merging the phases of play instead would put
-- 3,030 of them on top of their neighbour.
--
-- Basketball is not checked here because its possessions are the provider's own
-- and 1,521 pairs of them share their boundary frame, with 2 genuinely
-- overlapping and 29 lasting a single frame. That is a fact about the bundle,
-- reported by the contract tests, not something a model may quietly repair.

WITH ordered AS (
    SELECT
        match_id,
        event_id,
        frame_start,
        lag(frame_end) OVER (PARTITION BY match_id ORDER BY frame_start) AS previous_end
    FROM {{ ref('events') }}
    WHERE sport = 'football' AND event_type = 'possession'
)

SELECT match_id, event_id, frame_start, previous_end
FROM ordered
WHERE previous_end IS NOT NULL AND frame_start <= previous_end
