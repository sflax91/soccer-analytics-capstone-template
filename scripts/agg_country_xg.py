import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "agg_country_xg.parquet")

duckdb.sql(f"""
                      SELECT shot_statsbomb_xg, shot, location_x_adj, location_y_adj, STATS_GROUP, Country, id
                      FROM read_parquet('{ADDITIONAL_DIR}/grouping_xg.parquet') xg
                      INNER JOIN read_parquet('{ADDITIONAL_DIR}/country_grouping.parquet') c
                        ON xg.GROUPING_PK = c.GROUPING_PK
                                """).write_parquet(output_path)

print('Aggregate Country xG Done.')