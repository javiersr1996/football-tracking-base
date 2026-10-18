-- Decision 6, both halves of it. A player never carries a z: basketball sends a
-- constant 0.0, which is not a measurement, and football sends nothing. The ball
-- always carries one, uncorrected, in both sports. A row that breaks either half
-- means someone started treating a filler as data.

SELECT sport, entity_kind, count(*) AS n_rows
FROM {{ ref('frames') }}
WHERE (entity_kind = 'player' AND z_m IS NOT NULL)
   OR (entity_kind = 'ball' AND z_m IS NULL)
GROUP BY ALL
