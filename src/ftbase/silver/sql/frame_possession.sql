-- Copied from tracklab@686508a: pipeline/dbt/models/silver/views/frame_possession.sql
-- Changes: the basketball paragraphs of the comment dropped; dbt ref() replaced by table
-- names. Needed by frames_attacking, so it comes along although the plan names only that
-- view.
--
-- Which team possession a frame belongs to, for the frames that belong to one.
--
-- It is a view and not a table on purpose: expanding the possessions to the
-- frames they span would write tens of millions of rows that are a range join
-- away from what silver already holds.
--
-- A frame inside two possessions goes to the one that starts latest, which on a
-- shared boundary is the one starting on it. Football, whose possessions this
-- project derives with the chain rule, has no such ties
-- (assert_football_possessions_do_not_overlap), so the tie-break never fires here;
-- it stays so that a tie, if a bundle ever produced one, is broken here rather
-- than left to whoever reads the view.

WITH match_frames AS (
    SELECT DISTINCT sport, match_id, frame_native FROM frames
)

SELECT
    f.sport,
    f.match_id,
    f.frame_native,
    e.event_id AS possession_id,
    e.team_id
FROM match_frames f
JOIN events e
    ON e.match_id = f.match_id
    AND e.event_type = 'possession'
    AND f.frame_native BETWEEN e.frame_start AND e.frame_end
QUALIFY row_number() OVER (
    PARTITION BY f.match_id, f.frame_native
    ORDER BY e.frame_start DESC
) = 1
