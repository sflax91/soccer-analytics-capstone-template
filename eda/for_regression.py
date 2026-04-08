import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
output_path = str(ADDITIONAL_DIR / "for_regression.parquet")

duckdb.sql(f"""
                      with event_data as (
                      SELECT match_id, period, possession, possession_team_id, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp
                      FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                      
                      ),
                      add_groups_possession as (
                      SELECT distinct event_data.match_id, event_data.period, event_data.possession, 
                      off_lineup.GROUPING_PK OFF_GROUPING_PK, def_lineup.GROUPING_PK DEF_GROUPING_PK, total_xg, possession_type, n_shots, duration_seconds,

                      off_lineup.BACKS OFF_BACKS, off_lineup.MIDFIELDERS OFF_MIDFIELDERS, off_lineup.FORWARDS OFF_FORWARDS, off_lineup.GOALKEEPER OFF_GOALKEEPER, 
                      off_lineup.C0 OFF_C0, off_lineup.C1 OFF_C1, off_lineup.C2 OFF_C2, off_lineup.C3 OFF_C3, off_lineup.C4 OFF_C4,

                      def_lineup.BACKS DEF_BACKS, def_lineup.MIDFIELDERS DEF_MIDFIELDERS, def_lineup.FORWARDS DEF_FORWARDS, def_lineup.GOALKEEPER DEF_GOALKEEPER, 
                      def_lineup.C0 DEF_C0, def_lineup.C1 DEF_C1, def_lineup.C2 DEF_C2, def_lineup.C3 DEF_C3, def_lineup.C4 DEF_C4
                      

                      FROM event_data
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') off_lineup
                        ON event_data.match_id = off_lineup.match_id
                        AND event_data.period = off_lineup.period
                        AND event_data.possession_team_id = off_lineup.team_id
                        AND event_timestamp >= off_lineup.interval_start
                        AND event_timestamp < IFNULL(off_lineup.interval_end, CURRENT_TIMESTAMP)
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') def_lineup
                        ON event_data.match_id = def_lineup.match_id
                        AND event_data.period = def_lineup.period
                        AND event_data.possession_team_id != def_lineup.team_id
                        AND event_timestamp >= def_lineup.interval_start
                        AND event_timestamp < IFNULL(def_lineup.interval_end, CURRENT_TIMESTAMP)
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/possession_types.parquet') pt
                        ON event_data.match_id = pt.match_id
                        AND event_data.period = pt.period
                        AND event_data.possession = pt.possession
                     ),
                     agg_all as (
                     SELECT possession_type, 
                     OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4,
                     DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4, SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_seconds) / 60 duration_minutes
                     FROM add_groups_possession
                     WHERE OFF_GROUPING_PK IS NOT NULL AND possession_type IS NOT NULL
                     GROUP BY possession_type, 
                     OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4,
                     DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4
                     )
                     SELECT *
                     FROM agg_all

                    """).write_parquet(output_path)
