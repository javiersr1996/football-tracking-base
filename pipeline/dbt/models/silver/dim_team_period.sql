-- Task 9 in silver keys. `attack_sign` is +1 when the team attacks towards +x of
-- the venue frame in that period and -1 otherwise, and `frames` never bakes it
-- in: the normalisation is one multiplication in the `frames_attacking` view
-- (decision 2). `evidence` travels with the row because a sign with no evidence
-- is an assertion, and this one was measured: basketball from
-- `possessions.leftHoop` with a shot probe per group on top, football from
-- `home_team_side` against the attacking side of the phases.

SELECT
    (s.id_prefix || ':' || o.native_match_id)::VARCHAR AS match_id,
    o.period::SMALLINT AS period,
    (s.id_prefix || ':' || o.native_team_id)::VARCHAR AS team_id,
    o.attack_sign::SMALLINT AS attack_sign,
    o.evidence::VARCHAR AS evidence
FROM {{ source('bronze_common', 'orientation') }} o
JOIN {{ ref('dim_sport') }} s ON s.sport = o.sport
