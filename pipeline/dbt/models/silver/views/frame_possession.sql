-- Which team possession a frame belongs to, for the frames that belong to one.
--
-- It is a view and not a table on purpose: expanding 5,562 possessions to the
-- frames they span would write tens of millions of rows that are a range join
-- away from what silver already holds.
--
-- **A possession ends on the frame the next one starts on**, in basketball:
-- 1,521 pairs of the 1,564 native possessions share their boundary frame. So a
-- frame can fall inside two possessions, and the tie is broken here rather than
-- left to whoever reads the table: it goes to the possession that starts latest,
-- which on a shared boundary is the one starting on it.
--
-- The tie-break also absorbs something that is not a boundary. Match 178442
-- carries one possession the provider labels period 4 whose frames begin 1,450
-- before period 4 does in the tracking, so for 429 frames it lies on top of the
-- possessions of period 3; there, the later-starting period 3 possessions win.
-- Two more basketball possessions overlap their neighbour outright and 29 last a
-- single frame. None of that is repaired here: it is a fact about the bundle,
-- counted and pinned by tests/contracts/test_events.py. Football, whose
-- possessions this project derives, has no ties at all.

WITH match_frames AS (
    SELECT DISTINCT sport, match_id, frame_native FROM {{ ref('frames') }}
)

SELECT
    f.sport,
    f.match_id,
    f.frame_native,
    e.event_id AS possession_id,
    e.team_id
FROM match_frames f
JOIN {{ ref('events') }} e
    ON e.match_id = f.match_id
    AND e.event_type = 'possession'
    AND f.frame_native BETWEEN e.frame_start AND e.frame_end
QUALIFY row_number() OVER (
    PARTITION BY f.match_id, f.frame_native
    ORDER BY e.frame_start DESC
) = 1
