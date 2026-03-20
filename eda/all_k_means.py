import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "all_k_means.parquet")

duckdb.sql(f"""
                      SELECT every_player.player_id, pct_shot_first_time, pct_shot_follows_dribble, pct_shot_open_goal, pct_shot_dominant_foot, 
                      pct_shot_header, pct_shot_into_goal pct_shot_goal_scored, pct_shot_saved pct_shot_taken_saved, shots_per_minute, 
                      goals_over_expected, pct_shot_q1 pct_shot_from_q1_dist, pct_shot_q2 pct_shot_from_q2_dist, 
                      pct_shot_q3 pct_shot_from_q3_dist, pct_shot_q4 pct_shot_from_q4_dist, pct_shots_taken_in_arc, pct_of_goals_from_arc, pct_shots_taken_in_box, pct_of_goals_from_box, 
                      avg_progress_to_goal_shooting_on_per_carry, avg_distance_traveled_per_carry, pct_carrying_team_possession, pct_miscontrol, 
                      pct_dispossessed, carries, pct_duel_won, duels_per_minute, foul_committed_per_minute, yellow_cards_per_minute, 
                      red_cards_per_minute, pressures_applied_per_minute defensive_pressures_applied_per_minute, blocks_per_minute defensive_blocks_per_minute, 
                      interceptions_per_minute defensive_interceptions_per_minute, 
                      MINUTES_ON_PITCH, pass_attempt, pass_success_pct, pass_attempts_per_minute, percent_cross_success, percent_through_ball_success, 
                      percent_through_ball percent_pass_through_ball, percent_cross percent_pass_cross, percent_q1 percent_q1_pass_dist, 
                      percent_q2 percent_q2_pass_dist, percent_q3 percent_q3_pass_dist, percent_q4 percent_q4_pass_dist, pass_shot_assist_per_minute
                      FROM (
                      SELECT player_id
                      FROM read_parquet('{ADDITIONAL_DIR}/shot_k_means.parquet') 

                      UNION

                      SELECT player_id
                      FROM read_parquet('{ADDITIONAL_DIR}/carry_k_means.parquet') 

                      UNION

                      SELECT player_id
                      FROM read_parquet('{ADDITIONAL_DIR}/defense_k_means.parquet') 

                      UNION

                      SELECT player_id
                      FROM read_parquet('{ADDITIONAL_DIR}/pass_k_means.parquet') 
                      ) every_player
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/shot_k_means.parquet') s
                        ON every_player.player_id = s.player_id
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/carry_k_means.parquet') c
                        ON every_player.player_id = c.player_id
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/defense_k_means.parquet') d
                        ON every_player.player_id = d.player_id
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/pass_k_means.parquet') p
                        ON every_player.player_id = p.player_id
                    """).write_parquet(output_path)
