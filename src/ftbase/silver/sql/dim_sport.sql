-- Copied from tracklab@686508a: pipeline/dbt/models/silver/dim_sport.sql
-- Changes: football only (the basketball row and its comments dropped).
--
-- The row that makes `sport` a column and not a fork in the code. Everything
-- here is a constant of the bundle, measured in tracklab and cited next to it; no
-- model is allowed to hard-code any of these values again.
--
-- native_event_frame is the one that changed with the evidence. The plan wrote
-- 'venue' for football as a hypothesis; tracklab's task 9 measured it and the
-- attack frame wins in 20 of 20 matches (94.66-100% within 1 m against
-- 40.04-56.41%), so football is 'attacking_pos_x': the attacker goes towards +x
-- and the rotation is chosen by `attacking_side`.

SELECT
    'football'::VARCHAR AS sport,
    'fb'::VARCHAR AS id_prefix,
    'm'::VARCHAR AS native_unit,
    1.0::DOUBLE AS unit_to_m,
    10::SMALLINT AS sample_rate_hz,          -- frame * 100 ms
    'elapsed'::VARCHAR AS clock_kind,        -- seconds since the period's first frame
    22::SMALLINT AS players_on_surface,
    'attacking_pos_x'::VARCHAR AS native_event_frame
