-- Every entity in `frames` has its row in `dim_entity`, for the same match. A
-- `relationships` test on `entity_id` alone would pass on a wrong match: the
-- ball is `bb:ball` in all ten basketball matches, so the key is the pair.

SELECT f.match_id, f.entity_id, count(*) AS n_rows
FROM {{ ref('frames') }} f
ANTI JOIN {{ ref('dim_entity') }} e
    ON e.match_id = f.match_id AND e.entity_id = f.entity_id
GROUP BY ALL
