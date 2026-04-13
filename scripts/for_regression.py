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
                      SELECT distinct event_data.match_id, event_data.period, event_data.possession, event_data.possession_team_id OFF_TEAM_ID, 
                      off_lineup.MEN_WOMEN OFF_MEN_WOMEN, off_lineup.GROUPING_PK OFF_GROUPING_PK, def_lineup.GROUPING_PK DEF_GROUPING_PK,

                      off_lineup.BACKS OFF_BACKS, off_lineup.MIDFIELDERS OFF_MIDFIELDERS, off_lineup.FORWARDS OFF_FORWARDS, off_lineup.GOALKEEPER OFF_GOALKEEPER, 
                      off_lineup.C0 OFF_C0, off_lineup.C1 OFF_C1, off_lineup.C2 OFF_C2, off_lineup.C3 OFF_C3, off_lineup.C4 OFF_C4, off_lineup.C5 OFF_C5, off_lineup.PLAYERS_ON_PITCH OFF_PLAYERS_ON_PITCH, 

                      def_lineup.team_id DEF_TEAM_ID, def_lineup.MEN_WOMEN DEF_MEN_WOMEN, def_lineup.BACKS DEF_BACKS, def_lineup.MIDFIELDERS DEF_MIDFIELDERS, def_lineup.FORWARDS DEF_FORWARDS, def_lineup.GOALKEEPER DEF_GOALKEEPER, 
                      def_lineup.C0 DEF_C0, def_lineup.C1 DEF_C1, def_lineup.C2 DEF_C2, def_lineup.C3 DEF_C3, def_lineup.C4 DEF_C4, def_lineup.C5 DEF_C5, off_lineup.PLAYERS_ON_PITCH DEF_PLAYERS_ON_PITCH
                      
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
                      
                     ),
                     agg_all as (
                     SELECT add_groups_possession.match_id, possession_type, add_groups_possession.period, add_groups_possession.possession, 
                     OFF_TEAM_ID, OFF_MEN_WOMEN, OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4, OFF_C5, OFF_PLAYERS_ON_PITCH,
                     DEF_TEAM_ID, DEF_MEN_WOMEN, DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4, DEF_C5, DEF_PLAYERS_ON_PITCH, SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_seconds) / 60 duration_minutes
                     FROM add_groups_possession
                     LEFT JOIN read_parquet('{ADDITIONAL_DIR}/possession_types.parquet') pt
                        ON add_groups_possession.match_id = pt.match_id
                        AND add_groups_possession.period = pt.period
                        AND add_groups_possession.possession = pt.possession
                        AND add_groups_possession.OFF_TEAM_ID = pt.possession_team_id
                     WHERE OFF_GROUPING_PK IS NOT NULL AND possession_type IS NOT NULL
                     GROUP BY add_groups_possession.match_id, possession_type, add_groups_possession.period, add_groups_possession.possession, 
                     DEF_TEAM_ID, OFF_TEAM_ID, OFF_MEN_WOMEN, OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4, OFF_C5, OFF_PLAYERS_ON_PITCH,
                     DEF_MEN_WOMEN, DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4, DEF_C5, DEF_PLAYERS_ON_PITCH
                     ),
                     iso_first_row as (
                     SELECT distinct match_id, possession_type, period, possession, 
                            OFF_TEAM_ID, OFF_MEN_WOMEN, OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4, OFF_C5, OFF_PLAYERS_ON_PITCH,
                            DEF_TEAM_ID, DEF_MEN_WOMEN, DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4, DEF_C5, DEF_PLAYERS_ON_PITCH, total_xg, n_shots, duration_minutes
                     FROM (
                            SELECT iso_full_squad.*, RANK() OVER (PARTITION BY match_id, off_team_id, period, possession ORDER BY match_id, off_team_id, period, possession, duration_minutes DESC) RANK_ROW
                            FROM (
                                    SELECT agg_all.*
                                    FROM agg_all
                                    WHERE OFF_PLAYERS_ON_PITCH = 11 AND OFF_PLAYERS_ON_PITCH = 11
                                    ) iso_full_squad
                            )
                     WHERE RANK_ROW = 1
                     )
                     SELECT distinct iso_first_row.*
                     FROM iso_first_row
                     LEFT JOIN (SELECT match_id, OFF_TEAM_ID, POSSESSION, period
                                FROM iso_first_row
                                GROUP BY match_id, OFF_TEAM_ID, POSSESSION, period
                                HAVING COUNT (*) > 1) dup_rows
                          ON iso_first_row.match_id = dup_rows.match_id
                          AND iso_first_row.OFF_TEAM_ID = dup_rows.OFF_TEAM_ID
                          AND iso_first_row.POSSESSION = dup_rows.POSSESSION
                          AND iso_first_row.period = dup_rows.period
                     WHERE dup_rows.match_id IS NULL

                    """).write_parquet(output_path)
print('For Regression Done.')