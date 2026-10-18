-- One row per match, 20 of them, and the place a reader goes to find out what a
-- match is made of before touching 22 million frames.
--
-- Three columns here are conclusions rather than copies, and each cites what
-- measured it:
--
-- * surface_*. Football declares its pitch and it is not the same in every
--   match (104, 105 and 106 m long, all 68 wide), so it is read per match from
--   match.json. Basketball declares nothing, so the court comes from task 9's
--   report: FIBA 28 x 15 m in all 10 matches, with the hoop at -40.75 ft, the
--   arc fitted at 22.15 ft and a shots.distance residual of 0.014 ft against
--   FIBA and 1.0 ft against 94x50. `surface_spec` names which one, and the
--   metres come from the seed so the rejected alternative stays visible.
-- * z_ball_status. 'biased' in basketball: apparent gravity about 15% low,
--   replicated in the 10 matches by the sister repo. 'unverified' in football:
--   the provider delivers ball_z whenever the ball is present (65-81% of the
--   frames of a match) and nothing here has checked it against physics yet.
--   That check is P3's, and silver corrects nothing either way (decision 6).
-- * z_player_status. 'constant_zero' in basketball, where the bundle sends 0.0
--   for every player in every frame, which is not a measurement; 'absent' in
--   football, which sends no z for players at all. Both become NULL in `frames`.
--
-- n_periods is counted from dim_period rather than declared, so the two models
-- cannot disagree about how many periods a match had.

WITH commits AS (
    SELECT
        s.source_id::VARCHAR AS source_id,
        s."commit"::VARCHAR AS source_commit
    FROM (SELECT unnest(sources) AS s FROM {{ source('manifest', 'sources') }})
),

basketball AS (
    SELECT
        'basketball' AS sport,
        m.id AS native_match_id,
        m.competition_name AS competition,
        m.season AS season,
        m.date_time AS date_time,
        m.home_team_id AS home_team_id,
        m.away_team_id AS away_team_id,
        m.home_score AS home_score,
        m.away_score AS away_score,
        r.court AS surface_spec,
        seed.length_m AS surface_length_m,
        seed.width_m AS surface_width_m
    FROM {{ source('bronze_basketball', 'matches') }} m
    JOIN {{ source('reports', 'orientation_basketball') }} r ON r.match_id = m.id
    JOIN {{ ref('surface_spec') }} seed ON seed.surface_spec = r.court
),

football AS (
    SELECT
        'football' AS sport,
        f.id AS native_match_id,
        f.competition_edition_competition_name AS competition,
        f.competition_edition_season_name AS season,
        f.date_time AS date_time,
        f.home_team_id AS home_team_id,
        f.away_team_id AS away_team_id,
        f.home_team_score AS home_score,
        f.away_team_score AS away_score,
        'provider_' || f.pitch_length || 'x' || f.pitch_width AS surface_spec,
        f.pitch_length AS surface_length_m,
        f.pitch_width AS surface_width_m
    FROM {{ source('bronze_football', 'match') }} f
),

matches AS (
    SELECT * FROM basketball
    UNION ALL
    SELECT * FROM football
),

periods AS (
    SELECT match_id, count(*) AS n_periods
    FROM {{ ref('dim_period') }}
    GROUP BY ALL
)

SELECT
    (s.id_prefix || ':' || m.native_match_id)::VARCHAR AS match_id,
    m.sport::VARCHAR AS sport,
    m.native_match_id::BIGINT AS native_match_id,
    m.competition::VARCHAR AS competition,
    m.season::VARCHAR AS season,
    strptime(m.date_time, '%Y-%m-%dT%H:%M:%SZ')::TIMESTAMP AS kickoff_utc,
    (s.id_prefix || ':' || m.home_team_id)::VARCHAR AS home_team_id,
    (s.id_prefix || ':' || m.away_team_id)::VARCHAR AS away_team_id,
    m.home_score::SMALLINT AS home_score,
    m.away_score::SMALLINT AS away_score,
    m.surface_length_m::DOUBLE AS surface_length_m,
    m.surface_width_m::DOUBLE AS surface_width_m,
    m.surface_spec::VARCHAR AS surface_spec,
    p.n_periods::SMALLINT AS n_periods,
    CASE m.sport WHEN 'basketball' THEN 'biased' ELSE 'unverified' END::VARCHAR AS z_ball_status,
    CASE m.sport WHEN 'basketball' THEN 'constant_zero' ELSE 'absent' END::VARCHAR AS z_player_status,
    c.source_commit::VARCHAR AS source_commit
FROM matches m
JOIN {{ ref('dim_sport') }} s ON s.sport = m.sport
JOIN periods p ON p.match_id = s.id_prefix || ':' || m.native_match_id
-- The sport's own bundle: a sport can have another source (football's body pose), whose
-- commit is not the one the match rows came from.
JOIN commits c ON c.source_id = 'opendata-' || m.sport
