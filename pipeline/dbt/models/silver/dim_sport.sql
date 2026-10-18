-- The two rows that make `sport` a column and not a fork in the code. Everything
-- here is a constant of the bundle, measured in fase 0 and cited next to it; no
-- model is allowed to hard-code any of these values again.
--
-- native_event_frame is the one that changed with the evidence. The plan wrote
-- 'venue' for football as a hypothesis; task 9 measured it and the attack frame
-- wins in 20 of 20 matches (94.66-100% within 1 m against 40.04-56.41%), so
-- football is 'attacking_pos_x': the attacker goes towards +x and the rotation
-- is chosen by `attacking_side`. Basketball is 'attacking_neg_x': the attacked
-- hoop sits at negative x and `possessions.leftHoop` chooses the rotation.

SELECT
    'basketball'::VARCHAR AS sport,
    'bb'::VARCHAR AS id_prefix,
    'ft'::VARCHAR AS native_unit,
    0.3048::DOUBLE AS unit_to_m,             -- exact by definition of the foot
    25::SMALLINT AS sample_rate_hz,          -- wallClock = frameIdx * 40 in all 1,472,932 frames
    'countdown'::VARCHAR AS clock_kind,      -- gameClock runs 600 -> 0, and 300 -> 0 in overtime
    10::SMALLINT AS players_on_surface,
    'attacking_neg_x'::VARCHAR AS native_event_frame

UNION ALL

SELECT
    'football',
    'fb',
    'm',
    1.0,
    10,                                      -- frame * 100 ms
    'elapsed',                               -- seconds since the period's first frame
    22,
    'attacking_pos_x'
