-- Exactly one ball per match, no more and no fewer. Two would double every
-- possession that joins through dim_entity; none would make the ball disappear
-- from a replay without any model failing.

SELECT
    m.match_id,
    count(e.entity_id) AS n_balls
FROM {{ ref('dim_match') }} m
LEFT JOIN {{ ref('dim_entity') }} e
    ON e.match_id = m.match_id AND e.entity_kind = 'ball'
GROUP BY ALL
HAVING count(e.entity_id) <> 1
