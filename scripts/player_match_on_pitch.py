import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "player_match_on_pitch.parquet")

possession_tl = duckdb.sql(f"""
                        SELECT player_id, match_id, SUM(date_diff('second', interval_start, interval_end)) / 60 MINUTES_ON_PITCH
                        FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet')
                        GROUP BY player_id, match_id
                                """).write_parquet(output_path)
print('Player Match on Pitch Done.')