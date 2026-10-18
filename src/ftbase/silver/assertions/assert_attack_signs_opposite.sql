-- The singular test of task 10, and the one that makes dim_team_period worth
-- having: in every (match, period) there are exactly two teams and their attack
-- signs add up to zero. A period with one team, three teams, or two teams
-- attacking the same way is a probe that lied, and it fails here rather than
-- silently mirroring half a match in `frames_attacking`.

SELECT
    match_id,
    period,
    count(*) AS n_teams,
    sum(attack_sign) AS sign_sum
FROM {{ ref('dim_team_period') }}
GROUP BY ALL
HAVING count(*) <> 2 OR sum(attack_sign) <> 0
