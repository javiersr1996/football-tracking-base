-- Copied from tracklab@686508a: pipeline/dbt/tests/assert_football_possessions_do_not_overlap.sql
-- Changes: the basketball paragraph of the comment dropped; dbt ref() replaced by the
-- table name; run by ftbase.silver.checks.
--
-- The invariant the chain rule was chosen for. The team possessions are derived
-- here, so nothing outside this project guarantees they tile the match without
-- stepping on each other; merging the phases of play instead would put some of
-- them on top of their neighbour.

WITH ordered AS (
    SELECT
        match_id,
        event_id,
        frame_start,
        lag(frame_end) OVER (PARTITION BY match_id ORDER BY frame_start) AS previous_end
    FROM events
    WHERE sport = 'football' AND event_type = 'possession'
)

SELECT match_id, event_id, frame_start, previous_end
FROM ordered
WHERE previous_end IS NOT NULL AND frame_start <= previous_end
