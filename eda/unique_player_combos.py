import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "unique_player_combos.parquet")

duckdb.sql(f"""
                        SELECT unique_combo.*, RANK () OVER (ORDER BY position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11) UNIT_GROUPING_RANK_ID
                         FROM (
                        SELECT distinct position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11
                         FROM read_parquet('{ADDITIONAL_DIR}/stack_lineup_groups.parquet') 
                         ) unique_combo

                         

      

                    """).write_parquet(output_path)
