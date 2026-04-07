import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "all_k_means.parquet")

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
                      )
           
                      SELECT every_player.player_id, MEN_WOMEN, 
                      IFNULL(pct_shot_first_time,0) pct_shot_first_time, 
                      IFNULL(pct_shot_follows_dribble,0) pct_shot_follows_dribble, 
                      IFNULL(pct_shot_open_goal,0) pct_shot_open_goal, 
                      IFNULL(pct_shot_dominant_foot,0) pct_shot_dominant_foot, 
                      IFNULL(pct_shot_header,0) pct_shot_header, 
                      IFNULL(pct_shot_into_goal,0) pct_shot_goal_scored, 
                      IFNULL(pct_shot_saved,0) pct_shot_taken_saved, 
                      IFNULL(shots_per_minute,0) shots_per_minute, 
                      IFNULL(goals_over_expected,0)goals_over_expected, 
                      IFNULL(pct_shot_q1,0) pct_shot_from_q1_dist, 
                      IFNULL(pct_shot_q2,0) pct_shot_from_q2_dist, 
                      IFNULL(pct_shot_q3,0) pct_shot_from_q3_dist, 
                      IFNULL(pct_shot_q4,0) pct_shot_from_q4_dist, 
                      IFNULL(pct_shots_taken_in_arc,0) pct_shots_taken_in_arc, 
                      IFNULL(pct_of_goals_from_arc,0) pct_of_goals_from_arc, 
                      IFNULL(pct_shots_taken_in_box,0) pct_shots_taken_in_box, 
                      IFNULL(pct_of_goals_from_box,0) pct_of_goals_from_box, 
                      IFNULL(avg_progress_to_goal_shooting_on_per_carry,0) avg_progress_to_goal_shooting_on_per_carry, 
                      IFNULL(avg_distance_traveled_per_carry,0) avg_distance_traveled_per_carry, 
                      IFNULL(pct_carrying_team_possession,0) pct_carrying_team_possession, 
                      IFNULL(pct_miscontrol,0) pct_miscontrol, 
                      IFNULL(pct_dispossessed,0) pct_dispossessed, 
                      IFNULL(carries,0) carries, 
                      IFNULL(pct_duel_won,0) pct_duel_won, 
                      IFNULL(duels_per_minute,0) duels_per_minute, 
                      IFNULL(foul_committed_per_minute,0) foul_committed_per_minute, 
                      IFNULL(yellow_cards_per_minute,0) yellow_cards_per_minute, 
                      IFNULL(red_cards_per_minute,0) red_cards_per_minute, 
                      IFNULL(pressures_applied_per_minute,0) defensive_pressures_applied_per_minute, 
                      IFNULL(blocks_per_minute,0) defensive_blocks_per_minute, 
                      IFNULL(interceptions_per_minute,0) defensive_interceptions_per_minute, 
                      IFNULL(MINUTES_ON_PITCH,0) MINUTES_ON_PITCH, 
                      IFNULL(pass_attempt,0) pass_attempt, 
                      IFNULL(pass_success_pct,0) pass_success_pct, 
                      IFNULL(pass_attempts_per_minute,0) pass_attempts_per_minute, 
                      IFNULL(percent_cross_success,0) percent_cross_success, 
                      IFNULL(percent_through_ball_success,0) percent_through_ball_success, 
                      IFNULL(percent_through_ball,0) percent_pass_through_ball, 
                      IFNULL(percent_cross,0) percent_pass_cross, 
                      IFNULL(percent_q1,0) percent_q1_pass_dist, 
                      IFNULL(percent_q2,0) percent_q2_pass_dist, 
                      IFNULL(percent_q3,0) percent_q3_pass_dist, 
                      IFNULL(percent_q4,0) percent_q4_pass_dist, 
                      IFNULL(pass_shot_assist_per_minute,0) pass_shot_assist_per_minute
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
                      LEFT JOIN man_woman
                        ON every_player.player_id = man_woman.player_id
                    """).write_parquet(output_path)
