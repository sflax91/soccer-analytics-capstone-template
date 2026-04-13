import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "carry_k_means.parquet")

duckdb.sql(f"""
                      with match_stats as (
                      SELECT c.player_id, c.match_id, c.possession_team_id,
                      SUM(IFNULL(duration,0)) total_seconds_carrying, 
                      --AVG(IFNULL(duration,0)) avg_seconds_carry, 
                      SUM(IFNULL(DISTANCE_TRAVELED,0)) total_distance_traveled_carry, 
                      --AVG(IFNULL(DISTANCE_TRAVELED,0)) avg_distance_traveled_carry, 
                      SUM(IFNULL(PROGRESS_TO_GOAL_SHOOTING_ON,0)) total_progress_to_goal_shooting_on_carry,
                      --AVG(IFNULL(PROGRESS_TO_GOAL_SHOOTING_ON,0)) avg_progress_to_goal_shooting_on_carry, 
                      COUNT(*) carries
                      FROM read_parquet('{ADDITIONAL_DIR}/carry.parquet') c
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/event_proximity.parquet') ep
                        ON c.id = ep.id
                      GROUP BY c.player_id, c.match_id, c.possession_team_id
                      ),
                      agg_player as (

                      SELECT match_stats.player_id, 
                       SUM(total_seconds_carrying) total_seconds_carrying, 
                       SUM(total_distance_traveled_carry) total_distance_traveled_carry,
                       SUM(team_seconds_possession_when_player_on_pitch) total_team_seconds_possession_when_player_on_pitch, 
                       SUM(total_progress_to_goal_shooting_on_carry) total_progress_to_goal_shooting_on_carry, 
                       SUM(carries) carries
                      FROM match_stats

                      LEFT JOIN (
                                  SELECT possession_team_id, pt.player_id, e.match_id, SUM(IFNULL(duration,0)) team_seconds_possession_when_player_on_pitch 
                                  FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                                  LEFT JOIN read_parquet('{ADDITIONAL_DIR}/player_match_timeline_with_score.parquet') pt
                                    ON e.match_id = pt.match_id
                                    AND e.period = pt.period
                                    AND possession_team_id = pt.team_id
                                    AND strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) >= interval_start
                                    AND strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) < interval_end
                                  GROUP BY possession_team_id, pt.player_id, e.match_id
                                  HAVING SUM(IFNULL(duration,0)) > 0
                                ) team_possession

                        ON match_stats.match_id = team_possession.match_id
                        AND match_stats.possession_team_id = team_possession.possession_team_id
                        AND match_stats.player_id = team_possession.player_id


                      GROUP BY match_stats.player_id

                      )
                      SELECT agg_player.player_id, 
                       CASE
                       WHEN IFNULL(total_progress_to_goal_shooting_on_carry,0) <= 0 OR IFNULL(carries,0) <= 0 THEN 0
                       ELSE IFNULL(total_progress_to_goal_shooting_on_carry,0) / IFNULL(carries,0)
                       END AS avg_progress_to_goal_shooting_on_per_carry, 
                       CASE
                       WHEN IFNULL(total_distance_traveled_carry,0) <= 0 OR IFNULL(carries,0) <= 0 THEN 0
                       ELSE IFNULL(total_distance_traveled_carry,0) / IFNULL(carries,0)
                       END AS avg_distance_traveled_per_carry, 
                       (total_seconds_carrying + other_possession_time) / total_team_seconds_possession_when_player_on_pitch pct_carrying_team_possession,
                       miscontrols / carries pct_miscontrol, dispossessed / carries pct_dispossessed,
                       carries
                       --, other_possession_time, dispossessed, bad_possession_time, miscontrols

                       FROM agg_player
                       LEFT JOIN (SELECT player_id, SUM(duration) / 60 bad_possession_time, 
                                    SUM(CASE WHEN type = 'Dispossessed' THEN 1 ELSE 0 END) dispossessed,
                                    SUM(CASE WHEN type = 'Miscontrol' THEN 1 ELSE 0 END) miscontrols
                                    FROM read_parquet('{STATSBOMB_DIR}/events.parquet') 
                                    WHERE type IN ('Dispossessed', 'Miscontrol') 
                                    GROUP BY player_id) bad_poss
                          ON agg_player.player_id = bad_poss.player_id
                        LEFT JOIN (SELECT player_id, SUM(duration) / 60 other_possession_time FROM read_parquet('{STATSBOMB_DIR}/events.parquet') WHERE type IN ('Dribble') GROUP BY player_id) other_poss
                          ON agg_player.player_id = other_poss.player_id
                    """).write_parquet(output_path)

print('Carry K Means Done.')