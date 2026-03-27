from pathlib import Path
import duckdb

DATA_DIR = Path.cwd().parent / "data"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"

output_path = ADDITIONAL_DIR / "possession_types.parquet"

duckdb.sql(f"""

WITH events_core AS (
    SELECT
        match_id,
        possession,
        possession_team_id,
        team,
        team_id,
        period,
        type,
        play_pattern,
        minute,
        second,
        location_x,
        location_y,
        shot_statsbomb_xg,
        minute * 60 + second AS event_time_sec
    FROM read_parquet('{STATSBOMB_DIR / "events.parquet"}')
    WHERE period != 5
),

team_period_direction AS (
    SELECT
        match_id,
        team_id,
        period,
        AVG(location_x) AS mean_raw_x,
        CASE
            WHEN AVG(location_x) >= 60 THEN -1
            ELSE 1
        END AS observed_direction
    FROM events_core
    WHERE location_x IS NOT NULL
    GROUP BY match_id, team_id, period
),

events_norm AS (
    SELECT
        e.*,
        CASE
            WHEN d.observed_direction = 1
                THEN e.location_x
            ELSE 120 - e.location_x
        END AS normalized_x
    FROM events_core e
    LEFT JOIN team_period_direction d
        ON e.match_id = d.match_id
       AND e.team_id = d.team_id
       AND e.period = d.period
),

possessions_geom AS (
    SELECT
        match_id,
        possession,
        possession_team_id,
        FIRST(normalized_x) AS start_x,
        MAX(normalized_x) AS max_x,
        MIN(normalized_x) AS min_x,
        COUNT(*) AS n_events,
        MIN(event_time_sec) AS start_time,
        MAX(event_time_sec) AS end_time,
        FIRST(team) AS team,
        FIRST(period) AS period,
        FIRST(play_pattern) AS play_pattern,
        MAX(event_time_sec) - MIN(event_time_sec) AS duration_seconds,
        MAX(normalized_x) - FIRST(normalized_x) AS max_progression,
        MIN(normalized_x) - FIRST(normalized_x) AS backward_progression
    FROM events_norm
    WHERE normalized_x IS NOT NULL
    GROUP BY match_id, possession, possession_team_id
),

possession_xg AS (
    SELECT
        match_id,
        possession,
        possession_team_id,
        SUM(shot_statsbomb_xg) AS total_xg,
        COUNT(*) AS n_shots
    FROM events_norm
    WHERE shot_statsbomb_xg IS NOT NULL
    GROUP BY match_id, possession, possession_team_id
),

possessions AS (
    SELECT
        g.*,
        COALESCE(x.total_xg, 0) AS total_xg,
        COALESCE(x.n_shots, 0) AS n_shots,
        CASE
            WHEN COALESCE(x.n_shots, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_shot
    FROM possessions_geom g
    LEFT JOIN possession_xg x
        ON g.match_id = x.match_id
       AND g.possession = x.possession
       AND g.possession_team_id = x.possession_team_id
),

possessions_zones AS (
    SELECT *,
        CASE
            WHEN start_x < 40 THEN 'Defensive Third'
            WHEN start_x < 80 THEN 'Middle Third'
            ELSE 'Attacking Third'
        END AS start_zone
    FROM possessions
),

possessions_progression AS (
    SELECT *,
        CASE
            WHEN max_progression >= 40 THEN 'Direct'
            WHEN duration_seconds >= 30 THEN 'Patient'
            ELSE 'Short'
        END AS progression_type
    FROM possessions_zones
),

possessions_classified AS (
    SELECT *,
        start_zone || ' - ' || progression_type AS possession_type
    FROM possessions_progression
)

SELECT *
FROM possessions_classified
WHERE possession_type != 'Attacking Third - Direct'

""").write_parquet(output_path)