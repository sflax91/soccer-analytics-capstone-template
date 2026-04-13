import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "pass_k_means.parquet")

duckdb.sql(f"""
                      with add_stats as (
                        SELECT player_id, match_id, --possession_team_id, DEF_TEAM_ID,
                        --COUNT(distinct match_id) games_against,
                        SUM(pass_goal_assist) pass_goal_assist, SUM(pass_shot_assist) pass_shot_assist, 
                        SUM(pass_cross_success) pass_cross_success, 
                        SUM(pass_cross_fail) pass_cross_fail,
                        SUM(pass_switch) pass_switch, 
                        SUM(pass_through_ball_success) pass_through_ball_success, 
                        SUM(pass_through_ball_fail) pass_through_ball_fail, 
                        SUM(pass_aerial_won) pass_aerial_won, 
                        SUM(pass_deflected) pass_deflected, 
                        SUM(pass_inswinging) pass_inswinging, 
                        SUM(pass_outswinging) pass_outswinging, 
                        --SUM(pass_no_touch) pass_no_touch, 
                        --SUM(pass_cut_back) pass_cut_back, 
                        SUM(pass_straight) pass_straight, 
                        SUM(pass_miscommunication) pass_miscommunication,
                        SUM(pass_success) pass_success,
                        SUM(pass_attempt) pass_attempt,
                        SUM(pos_pct_q1) pos_pct_q1,
                        SUM(pos_pct_q2) pos_pct_q2,
                        SUM(pos_pct_q3) pos_pct_q3,
                        SUM(pos_pct_q4) pos_pct_q4,
                        SUM(pos_pct_q1) pos_pct_q1_success,
                        SUM(pos_pct_q2) pos_pct_q2_success,
                        SUM(pos_pct_q3) pos_pct_q3_success,
                        SUM(pos_pct_q4) pos_pct_q4_success,
                      --,

                        SUM(pass_through_ball_success) + SUM(pass_through_ball_fail) pass_through_ball_attempts,
                        SUM(pass_cross_success) + SUM(pass_cross_fail) pass_cross_attempts
                      --,
                        --SUM(pass_inswinging) / SUM(pass_attempt) percent_inswinging,
                        --SUM(pass_outswinging) / SUM(pass_attempt) percent_outswinging,
                        --SUM(pass_switch) / SUM(pass_attempt) percent_switch,
                        --SUM(pass_cut_back) / SUM(pass_attempt) percent_cut_back,
                        --SUM(pass_straight) / SUM(pass_attempt) percent_straight

                        FROM read_parquet('{ADDITIONAL_DIR}/pass_level_stats.parquet') 
                        WHERE possession_team_id != DEF_TEAM_ID
                        GROUP BY player_id, match_id--,possession_team_id, DEF_TEAM_ID
                        ),
                        correct_cross as (
                        SELECT add_stats.*,
                        CASE WHEN IFNULL(pass_cross_attempts,0) < IFNULL(pass_cross_success,0) THEN IFNULL(pass_cross_success,0)
                        ELSE IFNULL(pass_cross_attempts,0)
                        END AS pass_cross_attempts_adj
                        FROM add_stats
                        
                        ),
                        add_time_on_pitch as (
                        SELECT correct_cross.player_id , SUM(MINUTES_ON_PITCH) MINUTES_ON_PITCH, 
                        SUM(pass_attempt) pass_attempt, SUM(pass_through_ball_attempts) pass_through_ball_attempts,
                        SUM(pass_attempt) / SUM(MINUTES_ON_PITCH) pass_attempts_per_minute, 
                        SUM(pass_through_ball_attempts) / SUM(MINUTES_ON_PITCH) pass_through_ball_attempts_per_minute,
                        SUM(pass_cross_attempts_adj) / SUM(MINUTES_ON_PITCH) pass_cross_attempts_per_minute,
                        SUM(pass_straight) / SUM(pass_attempt) percent_straight,
                        SUM(pass_deflected) / SUM(pass_attempt) percent_deflected,
                        SUM(pass_inswinging) / SUM(pass_attempt) percent_inswinging,
                        SUM(pass_outswinging) / SUM(pass_attempt) percent_outswinging,
                        SUM(pass_through_ball_attempts) pass_through_ball_attempts,
                        SUM(pass_cross_attempts_adj) pass_cross_attempts_adj,
                        SUM(pass_success) / SUM(pass_attempt) pass_success_pct,
                        SUM(IFNULL(pass_cross_success,0)) pass_cross_success,
                        SUM(IFNULL(pass_through_ball_success,0)) pass_through_ball_success,
                        --SUM(pos_pct_q1) pos_pct_q1,
                        --SUM(pos_pct_q2) pos_pct_q2,
                        --SUM(pos_pct_q3) pos_pct_q3,
                        --SUM(pos_pct_q4) pos_pct_q4,
                        SUM(pos_pct_q1) / SUM(pass_attempt) percent_q1,
                        SUM(pos_pct_q2) / SUM(pass_attempt) percent_q2,
                        SUM(pos_pct_q3) / SUM(pass_attempt) percent_q3,
                        SUM(pos_pct_q4) / SUM(pass_attempt) percent_q4,
                        SUM(pass_shot_assist) / SUM(MINUTES_ON_PITCH) pass_shot_assist_per_minute
                        FROM correct_cross
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/player_match_on_pitch.parquet') p
                           ON correct_cross.player_id = p.player_id
                           AND correct_cross.match_id = p.match_id
                        GROUP BY correct_cross.player_id
                        )

                        SELECT player_id, MINUTES_ON_PITCH, pass_attempt, pass_success_pct, pass_attempts_per_minute,
                        
                        CASE WHEN pass_cross_attempts_adj <= 0 THEN 0
                        ELSE pass_cross_success / pass_cross_attempts_adj
                        END AS percent_cross_success,
                        CASE WHEN pass_through_ball_attempts <= 0 THEN 0
                        ELSE pass_through_ball_success / pass_through_ball_attempts
                        END AS percent_through_ball_success,
                        CASE WHEN pass_through_ball_attempts <= 0 THEN 0
                        ELSE pass_through_ball_attempts / pass_attempt
                        END AS percent_through_ball,
                        CASE WHEN pass_cross_attempts_adj <= 0 THEN 0
                        ELSE pass_cross_attempts_adj / pass_attempt
                        END AS percent_cross,

                        percent_q1, percent_q2, percent_q3, percent_q4,  pass_shot_assist_per_minute
                        FROM add_time_on_pitch
                    """).write_parquet(output_path)
print('Pass K Means Done.')