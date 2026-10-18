-- Copied from tracklab@686508a: pipeline/dbt/models/silver/dim_team_period.sql
-- Changes: reads `orientation`, the table ftbase.silver.orientation computes in the same
-- run, instead of tracklab's bronze.common.orientation; basketball dropped from the
-- comment; dbt source() and ref() replaced by table names.
--
-- The orientation in silver keys. `attack_sign` is +1 when the team attacks
-- towards +x of the venue frame in that period and -1 otherwise, and `frames`
-- never bakes it in: the normalisation is one multiplication in the
-- `frames_attacking` view. `evidence` travels with the row because a sign with no
-- evidence is an assertion, and this one is checked: `home_team_side` against the
-- attacking side of the phases of play.

SELECT
    (s.id_prefix || ':' || o.native_match_id)::VARCHAR AS match_id,
    o.period::SMALLINT AS period,
    (s.id_prefix || ':' || o.native_team_id)::VARCHAR AS team_id,
    o.attack_sign::SMALLINT AS attack_sign,
    o.evidence::VARCHAR AS evidence
FROM orientation o
JOIN dim_sport s ON s.sport = o.sport
