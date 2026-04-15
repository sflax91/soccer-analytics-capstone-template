import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "more_shot_level_stats.parquet")

duckdb.sql(f"""
                      with goal_zone_metrics as (
                      SELECT player_id, OFF_ZONE, 
                      SUM(shot_statsbomb_xg) shot_statsbomb_xg_sum, SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) GOALS_CONVERTED, COUNT(*) shots_taken
                      --, 
                      --AVG(shot_statsbomb_xg) shot_statsbomb_xg_avg
                      --player_id, shot_statsbomb_xg, shot_outcome, OFF_ZONE, DIST_TO_GOAL
                      FROM read_parquet('{ADDITIONAL_DIR}/shot_level_stats.parquet')
                      GROUP BY player_id, OFF_ZONE
                      ),
                      player_level as (
                      SELECT player_id, 
                      SUM(shot_statsbomb_xg) shot_statsbomb_xg_sum, SUM(CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END) GOALS_CONVERTED, COUNT(*) shots_taken
                      FROM read_parquet('{ADDITIONAL_DIR}/shot_level_stats.parquet') 
                      GROUP BY player_id
                      ),
                      zone_stats as (
                      SELECT goal_zone_metrics.*, goals_converted / shot_statsbomb_xg_sum goals_over_xg, goals_converted - shot_statsbomb_xg_sum goals_minus_xg, goals_converted/shots_taken goal_shot_percentage, (goals_converted - shot_statsbomb_xg_sum) / shots_taken new_metric
                      FROM goal_zone_metrics
                      ),
                      player_max as (
                      SELECT zone_stats.player_id, OFF_ZONE
                      FROM zone_stats 
                      INNER JOIN (
                      SELECT player_id, MAX(new_metric) max_new_metric
                      FROM zone_stats
                      GROUP BY player_id) get_max 
                      ON zone_stats.player_id = get_max.player_id
                      AND zone_stats.new_metric = get_max.max_new_metric
                      ),
                      player_stats as (
                      SELECT player_level.*, goals_converted / shot_statsbomb_xg_sum goals_over_xg, goals_converted - shot_statsbomb_xg_sum goals_minus_xg, goals_converted/shots_taken goal_shot_percentage, (goals_converted - shot_statsbomb_xg_sum) / shots_taken new_metric
                      FROM player_level
                      )
                      SELECT player_stats.*, OFF_ZONE BEST_SHOT_ZONE
                      FROM player_stats
                      LEFT JOIN player_max
                        ON player_stats.player_id = player_max.player_id
                      WHERE shots_taken > 10
                      ORDER BY new_metric DESC
                    """).write_parquet(output_path)
