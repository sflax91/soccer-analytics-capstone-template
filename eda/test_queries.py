import duckdb
from pathlib import Path


EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "team_formation_timeline.parquet")



# y_coords = duckdb.sql(f"""

#                       SELECT *
#                       --GK, BACKS, MIDFIELDERS, FORWARDS, ATTACKING_MIDFIELDERS, DEFENDING_MIDFIELDERS, CENTER_FORWARDS, PLAYERS_ON_PITCH
#                       FROM read_parquet('{project_location}/eda/team_formation_timeline.parquet')
#                       WHERE PLAYERS_ON_PITCH > 11 OR PLAYERS_ON_PITCH < 9
#                       ORDER BY PLAYERS_ON_PITCH

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

y_coords = duckdb.sql(f"""

                      SELECT match_id, period, interval_start, interval_end, BACKS, MIDFIELDERS, FORWARDS,
                      --ATTACKING_MIDFIELDERS, DEFENDING_MIDFIELDERS, 
                      OVERALL_FORMATION, PLAYERS_ON_PITCH, TEAM_COMPOSITION_PK
                      FROM read_parquet('{ADDITIONAL_DIR}/team_composition.parquet')

                     """)#.write_csv('lineup_check.csv')


# y_coords = duckdb.sql(f"""

#                       SELECT distinct shot_outcome
#                       FROM read_parquet('{STATSBOMB_DIR}/events.parquet')

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)


y_coords = duckdb.sql(f"""

                         with lineup_check as (
                        SELECT match_id, team_id, period, interval_start, interval_end, BACKS, MIDFIELDERS, FORWARDS, OVERALL_FORMATION, PLAYERS_ON_PITCH, TEAM_COMPOSITION_PK
                        FROM read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') 
                        ),
                        event_times as (
                        SELECT id, match_id, team_id, duration, possession, possession_team_id, shot_outcome, period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, shot_statsbomb_xg
                        FROM read_parquet('{STATSBOMB_DIR}/events.parquet') 
                        WHERE team_id = possession_team_id
                        ),
                        get_stats as (
                        SELECT distinct et.id, et.match_id, et.possession, IFNULL(duration,0) duration, 
                        lc.team_id EVENT_TEAM_ID,
                        lc.TEAM_COMPOSITION_PK EVENT_TEAM_COMPOSITION_PK, lc.PLAYERS_ON_PITCH EVENT_TEAM_PLAYERS_ON_PITCH, 
                        lc.BACKS EVENT_TEAM_BACKS, lc.MIDFIELDERS EVENT_TEAM_MIDFIELDERS, 
                        lc.FORWARDS EVENT_TEAM_FORWARDS, lc.OVERALL_FORMATION EVENT_TEAM_OVERALL_FORMATION, IFNULL(shot_statsbomb_xg,0) event_team_shot_statsbomb_xg,
                        CASE WHEN et.team_id = et.possession_team_id AND UPPER(shot_outcome) = 'GOAL' THEN 1 ELSE 0 END AS event_team_goal_scored,

                        opponent.team_id OPPONENT_TEAM_ID,
                        opponent.TEAM_COMPOSITION_PK OPPONENT_TEAM_COMPOSITION_PK, opponent.PLAYERS_ON_PITCH OPPONENT_TEAM_PLAYERS_ON_PITCH, 
                        opponent.BACKS OPPONENT_TEAM_BACKS, opponent.MIDFIELDERS OPPONENT_TEAM_MIDFIELDERS, 
                        opponent.FORWARDS OPPONENT_TEAM_FORWARDS, opponent.OVERALL_FORMATION OPPONENT_TEAM_OVERALL_FORMATION
                        FROM event_times et
                        LEFT JOIN lineup_check lc
                           ON et.match_id = lc.match_id
                           AND et.team_id = lc.team_id
                           AND et.period = lc.period
                           AND event_timestamp >= lc.interval_start
                           AND event_timestamp < lc.interval_end
                        LEFT JOIN lineup_check opponent
                           ON et.match_id = opponent.match_id
                           AND et.team_id = opponent.team_id
                           AND et.period = opponent.period
                           AND event_timestamp >= opponent.interval_start
                           AND event_timestamp < opponent.interval_end
                        ),
                        agg_possession as (
                        SELECT possession, EVENT_TEAM_ID, match_id,
                        EVENT_TEAM_ID, 
                        EVENT_TEAM_COMPOSITION_PK, 
                        EVENT_TEAM_PLAYERS_ON_PITCH, 
                        EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, 
                        EVENT_TEAM_OVERALL_FORMATION,


                        SUM(event_team_goal_scored) event_team_goal_scored,
                        SUM(event_team_shot_statsbomb_xg) event_team_shot_statsbomb_xg,
                        SUM(duration) duration,

                        OPPONENT_TEAM_ID, 
                        OPPONENT_TEAM_COMPOSITION_PK, 
                        OPPONENT_TEAM_PLAYERS_ON_PITCH, 
                        OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS, 
                        OPPONENT_TEAM_OVERALL_FORMATION
                        FROM get_stats 

                        GROUP BY possession, EVENT_TEAM_ID, match_id,
                        EVENT_TEAM_ID, EVENT_TEAM_COMPOSITION_PK, EVENT_TEAM_PLAYERS_ON_PITCH, EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, EVENT_TEAM_OVERALL_FORMATION,
                        OPPONENT_TEAM_ID, OPPONENT_TEAM_COMPOSITION_PK, OPPONENT_TEAM_PLAYERS_ON_PITCH, OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS, OPPONENT_TEAM_OVERALL_FORMATION
                        ),
                        get_poss_type as (
                        SELECT agg_possession.*, possession_type
                        FROM agg_possession
                        LEFT JOIN (SELECT match_id, possession, possession_team_id, possession_type
                                    FROM read_parquet('{ADDITIONAL_DIR}/possession_types.parquet')) pt
                            ON agg_possession.match_id = pt.match_id
                            AND agg_possession.possession = pt.possession
                        )

                        SELECT 
                        --EVENT_TEAM_ID, EVENT_TEAM_COMPOSITION_PK, EVENT_TEAM_PLAYERS_ON_PITCH, 
                        EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, possession_type,
                        --EVENT_TEAM_OVERALL_FORMATION,
                        
                        SUM(event_team_goal_scored) event_team_goal_scored,
                        SUM(event_team_shot_statsbomb_xg) event_team_shot_statsbomb_xg,
                        SUM(duration) duration,

                        --OPPONENT_TEAM_ID, OPPONENT_TEAM_COMPOSITION_PK, OPPONENT_TEAM_PLAYERS_ON_PITCH, 
                        OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS--, 
                        --OPPONENT_TEAM_OVERALL_FORMATION


                        FROM get_poss_type

                        GROUP BY

                        EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, possession_type,
                        OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS

                        --EVENT_TEAM_ID, EVENT_TEAM_COMPOSITION_PK, EVENT_TEAM_PLAYERS_ON_PITCH, EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, EVENT_TEAM_OVERALL_FORMATION,
                        --OPPONENT_TEAM_ID, OPPONENT_TEAM_COMPOSITION_PK, OPPONENT_TEAM_PLAYERS_ON_PITCH, OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS, OPPONENT_TEAM_OVERALL_FORMATION

                     """)#.write_csv('lineup_check.csv')

print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT match_id, possession, possession_team_id, possession_type
#                       FROM read_parquet('{ADDITIONAL_DIR}/possession_types.parquet')

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)