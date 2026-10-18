-- Copied from tracklab@686508a: pipeline/dbt/tests/assert_frames_z_only_on_the_ball.sql
-- Changes: the basketball half of the comment dropped; dbt ref() replaced by the table
-- name; run by ftbase.silver.checks.
--
-- Both halves of the z rule. A player never carries a z: football sends nothing.
-- The ball always carries one, uncorrected. A row that breaks either half means
-- someone started treating a filler as data.

SELECT sport, entity_kind, count(*) AS n_rows
FROM frames
WHERE (entity_kind = 'player' AND z_m IS NOT NULL)
   OR (entity_kind = 'ball' AND z_m IS NULL)
GROUP BY ALL
