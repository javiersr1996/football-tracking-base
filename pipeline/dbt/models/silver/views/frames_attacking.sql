-- The frames of a team possession, turned so the team with the ball attacks
-- towards +x. It is the only attack normalisation in silver (decision 2): the
-- venue frame stays in `frames`, and the turn is one multiplication by the sign
-- `dim_team_period` measured for the team in possession in that period.
--
-- An inner join on purpose. A frame no possession claims has no attacking team,
-- so there is nothing to turn it towards; it stays in `frames` and is absent
-- here, never present with a guessed sign. `team_id` is still the entity's own
-- team, and the attacking one is `team_in_possession_id`: in a frame both teams
-- are on the surface and only one of them attacks.

SELECT
    f.*,
    p.possession_id,
    p.team_id AS team_in_possession_id,
    tp.attack_sign,
    tp.attack_sign * f.x_m AS x_att_m,
    tp.attack_sign * f.y_m AS y_att_m
FROM {{ ref('frames') }} f
JOIN {{ ref('frame_possession') }} p
    ON p.match_id = f.match_id AND p.frame_native = f.frame_native
JOIN {{ ref('dim_team_period') }} tp
    ON tp.match_id = f.match_id AND tp.period = f.period AND tp.team_id = p.team_id
