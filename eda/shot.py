import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "shot.parquet")

duckdb.sql(f"""
                        SELECT match_id, id, index_num, period, timestamp, duration, location_x, location_y, possession, possession_team_id, team_id, 
                         player_id, shot_end_location_x, shot_end_location_y, shot_end_location_z, shot_statsbomb_xg, shot_outcome, shot_technique, 
                         shot_body_part, shot_type, 
                         CASE WHEN shot_first_time THEN 1 ELSE 0 END AS shot_first_time, 
                         CASE WHEN shot_deflected THEN 1 ELSE 0 END AS shot_deflected, 
                         CASE WHEN shot_aerial_won THEN 1 ELSE 0 END AS shot_aerial_won, 
                         CASE WHEN shot_follows_dribble THEN 1 ELSE 0 END AS shot_follows_dribble, 
                         CASE WHEN shot_one_on_one THEN 1 ELSE 0 END AS shot_one_on_one,
                         CASE WHEN shot_open_goal THEN 1 ELSE 0 END AS shot_open_goal, 
                         CASE WHEN shot_redirect THEN 1 ELSE 0 END AS shot_redirect, 
                         CASE WHEN shot_saved_off_target THEN 1 ELSE 0 END AS shot_saved_off_target, 
                         CASE WHEN shot_saved_to_post THEN 1 ELSE 0 END AS shot_saved_to_post
                        FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                        WHERE shot_end_location_x IS NOT NULL
                                """).write_parquet(output_path)

