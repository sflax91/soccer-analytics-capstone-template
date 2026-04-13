import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ANALYSIS_DIR / "player_grouping_mapping.parquet")

duckdb.sql(f"""
                      SELECT pl.*, cluster, pl_adv.MEN_WOMEN, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, C0, C1, C2, C3, PLAYERS_ON_PITCH, GROUPING_PK
                      FROM (SELECT match_id, player_id, team_id, period, interval_start, interval_end, position_type
                      FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet')
                      ) pl

                      LEFT JOIN read_parquet('{ANALYSIS_DIR}/Mens_Clustering.parquet') mc
                        ON pl.player_id = mc.player_id
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') pl_adv
                        ON pl.match_id = pl_adv.match_id
                        AND pl.team_id = pl_adv.team_id
                        AND pl.period = pl_adv.period
                        AND pl.interval_start >= pl_adv.interval_start
                        AND pl.interval_start < pl_adv.interval_end
                      WHERE IFNULL(pl_adv.MEN_WOMEN,'-') = 'M'
                                """).write_parquet(output_path)

print('Player Grouping Mapping Done.')