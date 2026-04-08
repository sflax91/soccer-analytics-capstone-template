import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "team_composition.parquet")


duckdb.sql(f"""
                              SELECT tg.*, GK, BACKS, MIDFIELDERS, FORWARDS, ATTACKING_MIDFIELDERS, DEFENDING_MIDFIELDERS, CENTER_FORWARDS, ATTACK_FORMATION, MIDFIELD_FORMATION, DEFENSE_FORMATION, OVERALL_FORMATION, PLAYERS_ON_PITCH,
                              RANK() OVER (ORDER BY tg.match_id, tg.team_id, tg.period, tg.interval_start) TEAM_COMPOSITION_PK
                              FROM read_parquet('{ADDITIONAL_DIR}/team_groupings_timeline.parquet')  tg
                              INNER JOIN read_parquet('{ADDITIONAL_DIR}/team_formation_timeline.parquet')  tf
                                ON tg.match_id = tf.match_id
                                AND tg.team_id = tf.team_id
                                AND tg.period = tf.period
                                AND tg.interval_start = tf.interval_start
                                AND tg.interval_end = tf.interval_end
                           
                    """).write_parquet(output_path)
