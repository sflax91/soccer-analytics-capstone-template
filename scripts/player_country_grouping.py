import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "player_country_grouping.parquet")

duckdb.sql(f"""
                      SELECT distinct m.player_id, GROUPING_PK, Country
                      FROM read_parquet('{ANALYSIS_DIR}/player_grouping_mapping.parquet') m
                      INNER JOIN read_csv('{ADDITIONAL_DIR}/WC_Data/WC_COMBINED_PLAYERS.csv') c
                        ON m.player_id = c.StatsbombID
                      WHERE MEN_WOMEN = 'M' AND PLAYERS_ON_PITCH = 11
                                """).write_parquet(output_path)

print('Player Country Grouping Done.')