import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "gk_k_means.parquet")

duckdb.sql(f"""
                      with id_leagues as (
                      SELECT player_id, 
                      CASE WHEN UPPER(competition) LIKE '%WOMEN%' THEN 1
                      WHEN competition = 'NWSL' THEN 1
                      ELSE 0
                      END AS womens_game
                      --player_id, is_international, competition
                      FROM (SELECT distinct match_id, player_id
                            FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')) l
                      INNER JOIN read_parquet('{STATSBOMB_DIR}/matches.parquet') m
                        ON l.match_id = m.match_id
                        ),
                        aggregate_leagues as (
                      SELECT player_id, SUM(womens_game) womens_game
                      FROM id_leagues
                      GROUP BY player_id
                      ),
                      man_woman as (
                      SELECT player_id, 
                      CASE 
                      WHEN womens_game > 0 THEN 'W'
                      WHEN  womens_game = 0 THEN 'M'
                      ELSE NULL
                      END AS MEN_WOMEN
                      FROM aggregate_leagues
                      ),
                      
                      shot_percentiles as (
                      SELECT PERCENTILE_DISC([0.25,0.5,0.75]) WITHIN GROUP (ORDER BY shot_statsbomb_xg) percentiles
                      FROM read_parquet('{ADDITIONAL_DIR}/gk_stats.parquet')
                      ),
                      apply_shot_percentile as (
                      SELECT gk.*,
                      CASE 
                      WHEN shot_statsbomb_xg <= (SELECT percentiles[1] FROM shot_percentiles ) THEN 'Q1' 
                      WHEN shot_statsbomb_xg <= (SELECT percentiles[2] FROM shot_percentiles ) THEN 'Q2' 
                      WHEN shot_statsbomb_xg <= (SELECT percentiles[3] FROM shot_percentiles ) THEN 'Q3' 
                      ELSE 'Q4' END AS shot_xg_range
                      FROM read_parquet('{ADDITIONAL_DIR}/gk_stats.parquet') gk
                      ),
                      categorize_shots as (
                      SELECT match_id, gk_player_id, gk_save, gk_collected, gk_keeper_sweeper, gk_goal_conceded, gk_smother, gk_punch, gk_shot_faced, 
                      gk_penalty_faced, gk_penalty_saved, gk_standing, gk_diving, gk_moving, gk_set, gk_prone, 
                      CASE WHEN shot_zone LIKE '%L%' AND (IFNULL(gk_shot_faced,0) > 0 OR IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END as shot_faced_gk_right_side,
                      CASE WHEN shot_zone LIKE '%R%' AND (IFNULL(gk_shot_faced,0) > 0 OR IFNULL(gk_save,0) > 0 )THEN 1 ELSE 0 END as shot_faced_gk_left_side, 
                      CASE WHEN IFNULL(shot_zone,'-') = '-' AND (IFNULL(gk_shot_faced,0) > 0 OR IFNULL(gk_save,0) > 0 )THEN 1 ELSE 0 END as shot_faced_gk_unk_side,
                      --CASE WHEN shot_zone LIKE '%L%' AND (IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END as save_gk_right_side,
                      --CASE WHEN shot_zone LIKE '%R%' AND (IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END as save_gk_left_side,  
                      CASE WHEN shot_xg_range = 'Q1' AND (IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END AS q1_shot_xg,
                      CASE WHEN shot_xg_range = 'Q2' AND (IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END AS q2_shot_xg,
                      CASE WHEN shot_xg_range = 'Q3' AND (IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END AS q3_shot_xg,
                      CASE WHEN shot_xg_range = 'Q4' AND (IFNULL(gk_save,0) > 0 ) THEN 1 ELSE 0 END AS q4_shot_xg
                      
                      FROM apply_shot_percentile
                      ),
                      match_level as (
                      SELECT match_id, gk_player_id, 
                      SUM(gk_save) gk_save, SUM(gk_collected) gk_collected, SUM(gk_keeper_sweeper) gk_keeper_sweeper, SUM(gk_goal_conceded) gk_goal_conceded, 
                      SUM(gk_smother) gk_smother, SUM(gk_punch) gk_punch, SUM(gk_shot_faced) gk_shot_faced, 
                      SUM(gk_penalty_faced) gk_penalty_faced, SUM(gk_penalty_saved) gk_penalty_saved, SUM(gk_standing) gk_standing, 
                      SUM(gk_diving) gk_diving, SUM(gk_moving) gk_moving, SUM(gk_set) gk_set, SUM(gk_prone) gk_prone, 
                      SUM(shot_faced_gk_right_side) shot_faced_gk_right_side, 
                      SUM(shot_faced_gk_left_side) shot_faced_gk_left_side,
                      SUM(shot_faced_gk_unk_side) shot_faced_gk_unk_side, 
                      --SUM(save_gk_right_side) save_gk_right_side, SUM(save_gk_left_side) save_gk_left_side, 
                      SUM(q1_shot_xg) q1_shot_xg, SUM(q2_shot_xg) q2_shot_xg, SUM(q3_shot_xg) q3_shot_xg, SUM(q4_shot_xg) q4_shot_xg
                      FROM categorize_shots
                      GROUP BY match_id, gk_player_id
                      ),
                      get_time as (
                      SELECT match_level.*, MINUTES_ON_PITCH
                      FROM match_level
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/player_match_on_pitch.parquet') pt
                        ON match_level.match_id - pt.match_id
                        AND gk_player_id = player_id
                      WHERE IFNULL(MINUTES_ON_PITCH,0) > 0
                      ),
                      compute_stats as (
                      SELECT gk_player_id, 
                      SUM(gk_save) / SUM(gk_shot_faced) gk_shot_save_pct,
                      CASE 
                      WHEN SUM(gk_penalty_saved) = 0 THEN 0
                      WHEN SUM(gk_penalty_faced) = 0 THEN 0
                      ELSE SUM(gk_penalty_saved) / SUM(gk_penalty_faced) 
                      END AS gk_penalty_save_pct,
                      SUM(q1_shot_xg) / SUM(gk_shot_faced) pct_q1_shot_xg, 
                      SUM(q2_shot_xg) / SUM(gk_shot_faced) pct_q2_shot_xg, 
                      SUM(q3_shot_xg) / SUM(gk_shot_faced) pct_q3_shot_xg, 
                      SUM(q4_shot_xg) / SUM(gk_shot_faced) pct_q4_shot_xg, 
                      SUM(gk_collected) / SUM(MINUTES_ON_PITCH) gk_collected_per_minute, 
                      SUM(gk_keeper_sweeper) / SUM(MINUTES_ON_PITCH) gk_keeper_sweeper_per_minute, 
                      SUM(gk_smother) / SUM(MINUTES_ON_PITCH) gk_smother_per_minute, 
                      SUM(gk_punch) / SUM(MINUTES_ON_PITCH) gk_punch_per_minute, 
                      SUM(gk_shot_faced) / SUM(MINUTES_ON_PITCH) gk_shot_faced_per_minute, 
                      SUM(gk_penalty_faced) / SUM(MINUTES_ON_PITCH) gk_penalty_faced_per_minute,
                      SUM(shot_faced_gk_right_side) / SUM(gk_shot_faced) pct_shot_from_gk_right,
                      SUM(shot_faced_gk_left_side) / SUM(gk_shot_faced) pct_shot_from_gk_left,
                      SUM(shot_faced_gk_unk_side) / SUM(gk_shot_faced) pct_shot_from_gk_unk
                      
                      FROM get_time
                      GROUP BY gk_player_id
                      )

                      SELECT compute_stats.*, MEN_WOMEN
                      FROM compute_stats
                      LEFT JOIN man_woman
                        ON gk_player_id = player_id
                    """).write_parquet(output_path)
print('GK K Means Done.')