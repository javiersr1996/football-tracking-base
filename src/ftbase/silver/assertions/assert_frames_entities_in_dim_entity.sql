-- Copied from tracklab@686508a: pipeline/dbt/tests/assert_frames_entities_in_dim_entity.sql
-- Changes: the comment speaks of the football ball; dbt ref() replaced by table names;
-- run by ftbase.silver.checks.
--
-- Every entity in `frames` has its row in `dim_entity`, for the same match. A
-- relationships check on `entity_id` alone would pass on a wrong match: the ball
-- is `fb:ball` in all twenty matches, so the key is the pair.

SELECT f.match_id, f.entity_id, count(*) AS n_rows
FROM frames f
ANTI JOIN dim_entity e
    ON e.match_id = f.match_id AND e.entity_id = f.entity_id
GROUP BY ALL
