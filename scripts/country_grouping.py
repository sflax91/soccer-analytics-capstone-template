import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "country_grouping.parquet")

duckdb.sql(f"""
                      with join_country as (
                      SELECT distinct GROUPING_PK, Country
                      FROM read_parquet('{ANALYSIS_DIR}/player_grouping_mapping.parquet') m
                      INNER JOIN read_csv('{ADDITIONAL_DIR}/WC_Data/WC_COMBINED_PLAYERS.csv') c
                        ON m.player_id = c.StatsbombID
                      WHERE MEN_WOMEN = 'M' AND PLAYERS_ON_PITCH = 11
                      )

                      SELECT join_country.*, grouping_minutes_on_pitch
                      FROM join_country
                      LEFT JOIN (SELECT GROUPING_PK, SUM(DATE_DIFF('second',interval_start,interval_end)) / 11 / 60 grouping_minutes_on_pitch
                                FROM read_parquet('{ADDITIONAL_DIR}/player_grouping_mapping.parquet') xg
                                GROUP BY GROUPING_PK) get_time
                        ON join_country.GROUPING_PK = get_time.GROUPING_PK

                                """).write_parquet(output_path)

print('Country Grouping Done.')