import duckdb
from pathlib import Path

 
EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"




output_path = str(ADDITIONAL_DIR / "pass.parquet")

duckdb.sql(f"""
                        SELECT match_id, id, index_num, period, timestamp, duration, location_x, location_y, possession, possession_team_id, team_id, 
                         player_id, pass_end_location_x, pass_end_location_y, pass_recipient_id, pass_length, pass_angle, pass_height, pass_body_part, pass_type, pass_outcome,
                         pass_technique, pass_assisted_shot_id, 
                         CASE WHEN pass_goal_assist = TRUE THEN 1 ELSE 0 END AS pass_goal_assist, 
                         CASE WHEN pass_shot_assist = TRUE THEN 1 ELSE 0 END AS pass_shot_assist, 
                         CASE WHEN pass_cross = TRUE THEN 1 ELSE 0 END AS pass_cross, 
                         CASE WHEN pass_switch = TRUE THEN 1 ELSE 0 END AS pass_switch, 
                         CASE WHEN pass_through_ball = TRUE THEN 1 ELSE 0 END AS pass_through_ball, 
                         CASE WHEN pass_aerial_won = TRUE THEN 1 ELSE 0 END AS pass_aerial_won, 
                         CASE WHEN pass_deflected = TRUE THEN 1 ELSE 0 END AS pass_deflected,
                         CASE WHEN pass_inswinging = TRUE THEN 1 ELSE 0 END AS pass_inswinging, 
                         CASE WHEN pass_outswinging = TRUE THEN 1 ELSE 0 END AS pass_outswinging, 
                         CASE WHEN pass_no_touch = TRUE THEN 1 ELSE 0 END AS pass_no_touch, 
                         CASE WHEN pass_cut_back = TRUE THEN 1 ELSE 0 END AS pass_cut_back, 
                         CASE WHEN pass_straight = TRUE THEN 1 ELSE 0 END AS pass_straight, 
                         CASE WHEN pass_miscommunication = TRUE THEN 1 ELSE 0 END AS pass_miscommunication,
                        strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_date
                        FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
                        WHERE pass_length IS NOT NULL
                                """).write_parquet(output_path)

print('Pass Done.')