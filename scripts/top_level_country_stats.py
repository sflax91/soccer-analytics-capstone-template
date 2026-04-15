import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "top_level_country_stats.parquet")

duckdb.sql(f"""
                      with event_data as (
                      SELECT match_id, period, possession, possession_team_id, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp
                      FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                      
                      ),
                      add_groups_possession as (
                      SELECT distinct event_data.match_id, event_data.period, event_data.possession, GROUPING_PK , possession_team_id OFF_TEAM_ID
                      
                      FROM event_data
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') off_lineup
                        ON event_data.match_id = off_lineup.match_id
                        AND event_data.period = off_lineup.period
                        AND event_data.possession_team_id = off_lineup.team_id
                        AND event_timestamp >= off_lineup.interval_start
                        AND event_timestamp < IFNULL(off_lineup.interval_end, CURRENT_TIMESTAMP)
                      ),
                      agg_possession as (
                      SELECT add_groups_possession.match_id, add_groups_possession.period, add_groups_possession.possession, GROUPING_PK, OFF_TEAM_ID, possession_type, SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_seconds) / 60 duration_minutes
                      FROM add_groups_possession
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/possession_types.parquet') pt
                        ON add_groups_possession.match_id = pt.match_id
                        AND add_groups_possession.period = pt.period
                        AND add_groups_possession.possession = pt.possession
                        AND add_groups_possession.OFF_TEAM_ID = pt.possession_team_id
                      GROUP BY add_groups_possession.match_id, add_groups_possession.period, add_groups_possession.possession, GROUPING_PK, OFF_TEAM_ID, possession_type
                      ),
                      iso_possession_type as (

                      SELECT GROUPING_PK, possession_type, SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_minutes) duration_minutes
                      FROM (
                            SELECT match_id, period, possession, OFF_TEAM_ID, GROUPING_PK, possession_type, total_xg, n_shots, duration_minutes
                            FROM (
                                  SELECT *, RANK() OVER (PARTITION BY match_id, period, possession, OFF_TEAM_ID ORDER BY match_id, period, possession, OFF_TEAM_ID, possession_type, duration_minutes DESC) RANK_ROW
                                  FROM agg_possession
                                  )
                            WHERE RANK_ROW = 1
                            )
                      GROUP BY GROUPING_PK, possession_type
                      ),
                      group_id_possession_type as (
                      SELECT GROUPING_PK,
                      SUM(CASE WHEN possession_type = 'Defensive Third - Short' THEN total_xg ELSE 0 END) AS defensive_third_short_xg,
                      SUM(CASE WHEN possession_type = 'Defensive Third - Short' THEN duration_minutes ELSE 0 END) AS defensive_third_short_min,
                      SUM(CASE WHEN possession_type = 'Defensive Third - Patient' THEN total_xg ELSE 0 END) AS defensive_third_patient_xg,
                      SUM(CASE WHEN possession_type = 'Defensive Third - Patient' THEN duration_minutes ELSE 0 END) AS defensive_third_patient_min,
                      SUM(CASE WHEN possession_type = 'Attacking Third - Short' THEN total_xg ELSE 0 END) AS attacking_third_short_xg,
                      SUM(CASE WHEN possession_type = 'Attacking Third - Short' THEN duration_minutes ELSE 0 END) AS attacking_third_short_min,
                      SUM(CASE WHEN possession_type = 'Middle Third - Short' THEN total_xg ELSE 0 END) AS middle_third_short_xg,
                      SUM(CASE WHEN possession_type = 'Middle Third - Short' THEN duration_minutes ELSE 0 END) AS middle_third_short_min,
                      SUM(CASE WHEN possession_type = 'Attacking Third - Patient' THEN total_xg ELSE 0 END) AS attacking_third_patient_xg,
                      SUM(CASE WHEN possession_type = 'Attacking Third - Patient' THEN duration_minutes ELSE 0 END) AS attacking_third_patient_min,
                      SUM(CASE WHEN possession_type = 'Middle Third - Direct' THEN total_xg ELSE 0 END) AS middle_third_direct_xg,
                      SUM(CASE WHEN possession_type = 'Middle Third - Direct' THEN duration_minutes ELSE 0 END) AS middle_third_direct_min,
                      SUM(CASE WHEN possession_type = 'Middle Third - Patient' THEN total_xg ELSE 0 END) AS middle_third_patient_xg,
                      SUM(CASE WHEN possession_type = 'Middle Third - Patient' THEN duration_minutes ELSE 0 END) AS middle_third_patient_min,
                      SUM(CASE WHEN possession_type = 'Defensive Third - Direct' THEN total_xg ELSE 0 END) AS defensive_third_direct_xg,
                      SUM(CASE WHEN possession_type = 'Defensive Third - Direct' THEN duration_minutes ELSE 0 END) AS defensive_third_direct_min
                      FROM iso_possession_type

                      GROUP BY GROUPING_PK
                      ),
                      player_group as (
                      SELECT distinct m.player_id, m.team_id, position_type, 
                      cluster, MEN_WOMEN, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, C0, C1, C2, C3, PLAYERS_ON_PITCH, GROUPING_PK, Country
                      FROM read_parquet('{ANALYSIS_DIR}/player_grouping_mapping.parquet') m
                      INNER JOIN read_csv('{ADDITIONAL_DIR}/WC_Data/WC_COMBINED_PLAYERS.csv') c
                        ON m.player_id = c.StatsbombID
                      WHERE MEN_WOMEN = 'M' AND PLAYERS_ON_PITCH = 11
                      ),
                      breakout_poss_type as (
                      SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, --C0, C1, C2, C3, PLAYERS_ON_PITCH, 
                      SUM(defensive_third_short_xg) defensive_third_short_xg, 
                      SUM(defensive_third_patient_xg) defensive_third_patient_xg, 
                      SUM(attacking_third_short_xg) attacking_third_short_xg, 
                      SUM(middle_third_short_xg) middle_third_short_xg,
                      SUM(attacking_third_patient_xg) attacking_third_patient_xg, 
                      SUM(middle_third_direct_xg) middle_third_direct_xg, 
                      SUM(middle_third_patient_xg) middle_third_patient_xg, 
                      SUM(defensive_third_direct_xg) defensive_third_direct_xg, 

                      IFNULL(SUM(defensive_third_short_min),0) defensive_third_short_min, 
                      IFNULL(SUM(defensive_third_patient_min),0) defensive_third_patient_min, 
                      IFNULL(SUM(attacking_third_short_min),0) attacking_third_short_min, 
                      IFNULL(SUM(middle_third_short_min),0) middle_third_short_min, 
                      IFNULL(SUM(attacking_third_patient_min),0) attacking_third_patient_min, 
                      IFNULL(SUM(middle_third_direct_min),0) middle_third_direct_min, 
                      IFNULL(SUM(middle_third_patient_min),0) middle_third_patient_min, 
                      IFNULL(SUM(defensive_third_direct_min),0) defensive_third_direct_min
                      FROM player_group
                      LEFT JOIN group_id_possession_type
                        ON player_group.GROUPING_PK = group_id_possession_type.GROUPING_PK
                      GROUP BY Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER--, C0, C1, C2, C3, PLAYERS_ON_PITCH
                      ),

                      total_minutes as (
                      SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, --C0, C1, C2, C3, PLAYERS_ON_PITCH, 
                      defensive_third_short_min, defensive_third_patient_min, attacking_third_short_min, middle_third_short_min, attacking_third_patient_min,  middle_third_direct_min, middle_third_patient_min, defensive_third_direct_min,
                      defensive_third_short_min + defensive_third_patient_min + attacking_third_short_min + middle_third_short_min + attacking_third_patient_min +  middle_third_direct_min + middle_third_patient_min + defensive_third_direct_min total_min
                      FROM breakout_poss_type

                      ),
                      country_formation_minutes as (
                      SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, SUM(total_min) total_min
                      FROM total_minutes
                      GROUP BY Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER
                      ),
                      country_minutes as (
                      SELECT Country, SUM(total_min) total_min
                      FROM total_minutes
                      GROUP BY Country
                      ),
                      country_possession_type_minutes as (
                      SELECT Country, SUM(defensive_third_short_min) defensive_third_short_min, SUM(defensive_third_patient_min) defensive_third_patient_min, SUM(attacking_third_short_min) attacking_third_short_min, SUM(middle_third_short_min) middle_third_short_min, 
                      SUM(attacking_third_patient_min) attacking_third_patient_min,  SUM(middle_third_direct_min) middle_third_direct_min, SUM(middle_third_patient_min) middle_third_patient_min, SUM(defensive_third_direct_min) defensive_third_direct_min
                      FROM total_minutes
                      GROUP BY Country
                      ),
                      possession_type_pct as (
                      SELECT cpt.Country, 
                      defensive_third_short_min / total_min defensive_third_short_pct,
                      defensive_third_patient_min / total_min defensive_third_patient_pct,
                      attacking_third_short_min / total_min attacking_third_short_pct,
                      middle_third_short_min / total_min middle_third_short_pct,
                      attacking_third_patient_min / total_min attacking_third_patient_pct,
                      middle_third_direct_min / total_min middle_third_direct_pct,
                      middle_third_patient_min / total_min middle_third_patient_pct,
                      defensive_third_direct_min / total_min defensive_third_direct_pct,
                      total_min
                      FROM country_possession_type_minutes cpt
                      LEFT JOIN country_minutes
                        ON cpt.Country = country_minutes.Country
                      ),
                      pct_formation as (
                      SELECT country_formation_minutes.Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, country_formation_minutes.total_min / country_minutes.total_min pct_of_time
                      FROM country_formation_minutes
                      LEFT JOIN country_minutes
                        ON country_formation_minutes.Country = country_minutes.Country

                      ),
                      top_formation as (
                      SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, pct_of_time
                      FROM (
                      SELECT pct_formation.*, RANK() OVER (PARTITION BY Country ORDER BY Country, pct_of_time DESC) RANK_PCT
                      FROM pct_formation
                      )
                      WHERE RANK_PCT = 1
                      )
                      SELECT top_formation.*, defensive_third_short_pct, defensive_third_patient_pct, attacking_third_short_pct,
                      middle_third_short_pct, attacking_third_patient_pct, middle_third_direct_pct, middle_third_patient_pct, defensive_third_direct_pct,
                      total_min
                      FROM top_formation
                      LEFT JOIN possession_type_pct
                        ON top_formation.Country = possession_type_pct.Country
                                """).write_parquet(output_path)

print('Top Level Country Stats Done.')