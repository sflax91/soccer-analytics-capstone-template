import duckdb
from pathlib import Path


EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "team_formation_timeline.parquet")



y_coords = duckdb.sql(f"""

                      
                      
                      SELECT GROUPING_PK, SUM(DATE_DIFF('second',interval_start,interval_end)) / 11 / 60 grouping_minutes_on_pitch
                      FROM read_parquet('{ADDITIONAL_DIR}/player_grouping_mapping.parquet') xg
                      GROUP BY GROUPING_PK
                      
                      
                     """)#.write_csv('lineup_check.csv')

print(y_coords)



# y_coords = duckdb.sql(f"""

#                       SELECT m.player_id, m.team_id,  position_type, cluster, MEN_WOMEN, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, C0, C1, C2, C3, PLAYERS_ON_PITCH, GROUPING_PK, Country, SUM((date_diff('second', interval_start, interval_end ))) duration
#                       FROM read_parquet('{ANALYSIS_DIR}/player_grouping_mapping.parquet') m
#                       INNER JOIN read_csv('{ADDITIONAL_DIR}/WC_Data/WC_COMBINED_PLAYERS.csv') c
#                         ON m.player_id = c.StatsbombID
#                         GROUP BY m.player_id, m.team_id, position_type, cluster, MEN_WOMEN, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, C0, C1, C2, C3, PLAYERS_ON_PITCH, GROUPING_PK, Country
                      

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)


# y_coords = duckdb.sql(f"""

#                       with event_data as (
#                       SELECT match_id, period, possession, possession_team_id, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp
#                       FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                      
#                       ),
#                       add_groups_possession as (
#                       SELECT distinct event_data.match_id, event_data.period, event_data.possession, GROUPING_PK , possession_team_id OFF_TEAM_ID
                      
#                       FROM event_data
#                       LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') off_lineup
#                         ON event_data.match_id = off_lineup.match_id
#                         AND event_data.period = off_lineup.period
#                         AND event_data.possession_team_id = off_lineup.team_id
#                         AND event_timestamp >= off_lineup.interval_start
#                         AND event_timestamp < IFNULL(off_lineup.interval_end, CURRENT_TIMESTAMP)
#                       ),
#                       agg_possession as (
#                       SELECT add_groups_possession.match_id, add_groups_possession.period, add_groups_possession.possession, GROUPING_PK, OFF_TEAM_ID, possession_type, SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_seconds) / 60 duration_minutes
#                       FROM add_groups_possession
#                       LEFT JOIN read_parquet('{ADDITIONAL_DIR}/possession_types.parquet') pt
#                         ON add_groups_possession.match_id = pt.match_id
#                         AND add_groups_possession.period = pt.period
#                         AND add_groups_possession.possession = pt.possession
#                         AND add_groups_possession.OFF_TEAM_ID = pt.possession_team_id
#                       GROUP BY add_groups_possession.match_id, add_groups_possession.period, add_groups_possession.possession, GROUPING_PK, OFF_TEAM_ID, possession_type
#                       ),
#                       iso_possession_type as (

#                       SELECT GROUPING_PK, possession_type, SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_minutes) duration_minutes
#                       FROM (
#                             SELECT match_id, period, possession, OFF_TEAM_ID, GROUPING_PK, possession_type, total_xg, n_shots, duration_minutes
#                             FROM (
#                                   SELECT *, RANK() OVER (PARTITION BY match_id, period, possession, OFF_TEAM_ID ORDER BY match_id, period, possession, OFF_TEAM_ID, possession_type, duration_minutes DESC) RANK_ROW
#                                   FROM agg_possession
#                                   )
#                             WHERE RANK_ROW = 1
#                             )
#                       GROUP BY GROUPING_PK, possession_type
#                       ),
#                       group_id_possession_type as (
#                       SELECT GROUPING_PK,
#                       SUM(CASE WHEN possession_type = 'Defensive Third - Short' THEN total_xg ELSE 0 END) AS defensive_third_short_xg,
#                       SUM(CASE WHEN possession_type = 'Defensive Third - Short' THEN duration_minutes ELSE 0 END) AS defensive_third_short_min,
#                       SUM(CASE WHEN possession_type = 'Defensive Third - Patient' THEN total_xg ELSE 0 END) AS defensive_third_patient_xg,
#                       SUM(CASE WHEN possession_type = 'Defensive Third - Patient' THEN duration_minutes ELSE 0 END) AS defensive_third_patient_min,
#                       SUM(CASE WHEN possession_type = 'Attacking Third - Short' THEN total_xg ELSE 0 END) AS attacking_third_short_xg,
#                       SUM(CASE WHEN possession_type = 'Attacking Third - Short' THEN duration_minutes ELSE 0 END) AS attacking_third_short_min,
#                       SUM(CASE WHEN possession_type = 'Middle Third - Short' THEN total_xg ELSE 0 END) AS middle_third_short_xg,
#                       SUM(CASE WHEN possession_type = 'Middle Third - Short' THEN duration_minutes ELSE 0 END) AS middle_third_short_min,
#                       SUM(CASE WHEN possession_type = 'Attacking Third - Patient' THEN total_xg ELSE 0 END) AS attacking_third_patient_xg,
#                       SUM(CASE WHEN possession_type = 'Attacking Third - Patient' THEN duration_minutes ELSE 0 END) AS attacking_third_patient_min,
#                       SUM(CASE WHEN possession_type = 'Middle Third - Direct' THEN total_xg ELSE 0 END) AS middle_third_direct_xg,
#                       SUM(CASE WHEN possession_type = 'Middle Third - Direct' THEN duration_minutes ELSE 0 END) AS middle_third_direct_min,
#                       SUM(CASE WHEN possession_type = 'Middle Third - Patient' THEN total_xg ELSE 0 END) AS middle_third_patient_xg,
#                       SUM(CASE WHEN possession_type = 'Middle Third - Patient' THEN duration_minutes ELSE 0 END) AS middle_third_patient_min,
#                       SUM(CASE WHEN possession_type = 'Defensive Third - Direct' THEN total_xg ELSE 0 END) AS defensive_third_direct_xg,
#                       SUM(CASE WHEN possession_type = 'Defensive Third - Direct' THEN duration_minutes ELSE 0 END) AS defensive_third_direct_min
#                       FROM iso_possession_type

#                       GROUP BY GROUPING_PK
#                       ),
#                       player_group as (
#                       SELECT distinct m.player_id, m.team_id, position_type, 
#                       cluster, MEN_WOMEN, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, C0, C1, C2, C3, PLAYERS_ON_PITCH, GROUPING_PK, Country
#                       FROM read_parquet('{ANALYSIS_DIR}/player_grouping_mapping.parquet') m
#                       INNER JOIN read_csv('{ADDITIONAL_DIR}/WC_Data/WC_COMBINED_PLAYERS.csv') c
#                         ON m.player_id = c.StatsbombID
#                       WHERE MEN_WOMEN = 'M' AND PLAYERS_ON_PITCH = 11
#                       ),
#                       breakout_poss_type as (
#                       SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, --C0, C1, C2, C3, PLAYERS_ON_PITCH, 
#                       SUM(defensive_third_short_xg) defensive_third_short_xg, 
#                       SUM(defensive_third_patient_xg) defensive_third_patient_xg, 
#                       SUM(attacking_third_short_xg) attacking_third_short_xg, 
#                       SUM(middle_third_short_xg) middle_third_short_xg,
#                       SUM(attacking_third_patient_xg) attacking_third_patient_xg, 
#                       SUM(middle_third_direct_xg) middle_third_direct_xg, 
#                       SUM(middle_third_patient_xg) middle_third_patient_xg, 
#                       SUM(defensive_third_direct_xg) defensive_third_direct_xg, 

#                       IFNULL(SUM(defensive_third_short_min),0) defensive_third_short_min, 
#                       IFNULL(SUM(defensive_third_patient_min),0) defensive_third_patient_min, 
#                       IFNULL(SUM(attacking_third_short_min),0) attacking_third_short_min, 
#                       IFNULL(SUM(middle_third_short_min),0) middle_third_short_min, 
#                       IFNULL(SUM(attacking_third_patient_min),0) attacking_third_patient_min, 
#                       IFNULL(SUM(middle_third_direct_min),0) middle_third_direct_min, 
#                       IFNULL(SUM(middle_third_patient_min),0) middle_third_patient_min, 
#                       IFNULL(SUM(defensive_third_direct_min),0) defensive_third_direct_min
#                       FROM player_group
#                       LEFT JOIN group_id_possession_type
#                         ON player_group.GROUPING_PK = group_id_possession_type.GROUPING_PK
#                       GROUP BY Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER--, C0, C1, C2, C3, PLAYERS_ON_PITCH
#                       ),

#                       total_minutes as (
#                       SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, --C0, C1, C2, C3, PLAYERS_ON_PITCH, 
#                       defensive_third_short_min, defensive_third_patient_min, attacking_third_short_min, middle_third_short_min, attacking_third_patient_min,  middle_third_direct_min, middle_third_patient_min, defensive_third_direct_min,
#                       defensive_third_short_min + defensive_third_patient_min + attacking_third_short_min + middle_third_short_min + attacking_third_patient_min +  middle_third_direct_min + middle_third_patient_min + defensive_third_direct_min total_min
#                       FROM breakout_poss_type

#                       ),
#                       country_formation_minutes as (
#                       SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, SUM(total_min) total_min
#                       FROM total_minutes
#                       GROUP BY Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER
#                       ),
#                       country_minutes as (
#                       SELECT Country, SUM(total_min) total_min
#                       FROM total_minutes
#                       GROUP BY Country
#                       ),
#                       country_possession_type_minutes as (
#                       SELECT Country, SUM(defensive_third_short_min) defensive_third_short_min, SUM(defensive_third_patient_min) defensive_third_patient_min, SUM(attacking_third_short_min) attacking_third_short_min, SUM(middle_third_short_min) middle_third_short_min, 
#                       SUM(attacking_third_patient_min) attacking_third_patient_min,  SUM(middle_third_direct_min) middle_third_direct_min, SUM(middle_third_patient_min) middle_third_patient_min, SUM(defensive_third_direct_min) defensive_third_direct_min
#                       FROM total_minutes
#                       GROUP BY Country
#                       ),
#                       possession_type_pct as (
#                       SELECT cpt.Country, 
#                       defensive_third_short_min / total_min defensive_third_short_pct,
#                       defensive_third_patient_min / total_min defensive_third_patient_pct,
#                       attacking_third_short_min / total_min attacking_third_short_pct,
#                       middle_third_short_min / total_min middle_third_short_pct,
#                       attacking_third_patient_min / total_min attacking_third_patient_pct,
#                       middle_third_direct_min / total_min middle_third_direct_pct,
#                       middle_third_patient_min / total_min middle_third_patient_pct,
#                       defensive_third_direct_min / total_min defensive_third_direct_pct,
#                       total_min
#                       FROM country_possession_type_minutes cpt
#                       LEFT JOIN country_minutes
#                         ON cpt.Country = country_minutes.Country
#                       ),
#                       pct_formation as (
#                       SELECT country_formation_minutes.Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, country_formation_minutes.total_min / country_minutes.total_min pct_of_time
#                       FROM country_formation_minutes
#                       LEFT JOIN country_minutes
#                         ON country_formation_minutes.Country = country_minutes.Country

#                       ),
#                       top_formation as (
#                       SELECT Country, BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, pct_of_time
#                       FROM (
#                       SELECT pct_formation.*, RANK() OVER (PARTITION BY Country ORDER BY Country, pct_of_time DESC) RANK_PCT
#                       FROM pct_formation
#                       )
#                       WHERE RANK_PCT = 1
#                       )
#                       SELECT *
#                       FROM top_formation
#                       LEFT JOIN possession_type_pct
#                         ON top_formation.Country = possession_type_pct.Country




#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT player_id, COUNT(*)
#                       FROM read_parquet('{ADDITIONAL_DIR}/all_k_means.parquet')
#                       GROUP BY player_id
#                       HAVING COUNT(*) > 1

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT MEN_WOMEN, COUNT(distinct player_id)
#                       FROM read_parquet('{ADDITIONAL_DIR}/all_k_means.parquet') pl
#                       GROUP BY MEN_WOMEN

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""
#                         with get_country_world_rank as (
#                          SELECT 
#                          lookup_id.*, conf.Confederation, 
#                          CASE 
#                          WHEN lookup_id.name LIKE '%Brazzaville%' THEN 134
#                          WHEN lookup_id.name LIKE '%Venezuela%' THEN 50
#                          WHEN lookup_id.name LIKE '%Korea%' AND lookup_id.name LIKE '%South%' THEN 22
#                          ELSE conf.World_Ranking_January_19_2026 
#                          END AS World_Ranking_January_19_2026
#                          FROM (SELECT id, name
#                                     FROM read_parquet('{STATSBOMB_DIR}/reference.parquet') 
#                                     WHERE table_name = 'country') lookup_id
#                          LEFT JOIN (SELECT *
#                                     FROM read_csv('{ADDITIONAL_DIR}/WC_2022.csv')) wc_2022
#                            ON lookup_id.name = wc_2022.country
#                          LEFT JOIN (SELECT *
#                                     FROM read_csv('{ADDITIONAL_DIR}/WC_2018.csv')) wc_2018
#                            ON lookup_id.name = wc_2018.country
#                         LEFT JOIN (SELECT *
#                                     FROM read_csv('{ADDITIONAL_DIR}/WC_Confederations.csv')) conf
#                            ON TRIM(lookup_id.name) = TRIM(conf.country)
#                         ),
#                         find_country_quartile as (
#                         SELECT distinct id country_id,  
#                         CASE 
#                         WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.25 THEN 'Q1'
#                         WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.5 THEN 'Q2'
#                         WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.75 THEN 'Q3'
#                         ELSE 'Q4'
#                         END AS COUNTRY_QUARTILE
#                         FROM get_country_world_rank
#                         )

#                         SELECT *
#                         FROM find_country
                         

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)


# y_coords = duckdb.sql(f"""
                        
#                          SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                          WHERE bad_behaviour_card IN ('Red Card', 'Second Yellow')
                      

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""
                        
#                          SELECT *
#                          FROM read_parquet('{ADDITIONAL_DIR}/stack_lineup_groups.parquet')
                      

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)


# y_coords = duckdb.sql(f"""
#                         with starting_xi as (
#                          SELECT from_period period, 0 as minute, 0 as second, match_id, team_id, player_id, IFNULL(player_nickname, player_name) player, 'Starting XI' as type, position_name, 
#                       NULL as substitution_replacement_id, NULL as substitution_replacement_name, NULL as substitution_outcome, NULL as bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')
#                          WHERE from_period = 1 AND from_time = '00:00'
#                       ),
#                       other_events as (
#                          SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                          WHERE type IN (
#                                         'Half End',
#                                         'Substitution'--,
#                                         --'Tactical Shift'
#                                         )
#                             OR bad_behaviour_card IN ('Red Card', 'Second Yellow')
#                             OR (type = 'Half Start' AND period > 1)
#                         ),
#                         add_tactical as (
#                         SELECT tactical_shift.*
#                         FROM (
#                               SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                               FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                               WHERE type = 'Tactical Shift' ) tactical_shift

#                         --LEFT JOIN other_events
#                            --ON tactical_shift.period = other_events.period
#                            --AND tactical_shift.minute = other_events.minute
#                            --AND tactical_shift.second = other_events.second
#                            --AND tactical_shift.team_id = other_events.team_id
#                            --AND tactical_shift.player_id = other_events.player_id
#                         --WHERE other_events.minute IS NULL
#                         ),
#                         combine_sources as (
#                         SELECT *
#                         FROM starting_xi

#                         UNION

#                         SELECT *
#                         FROM add_tactical

#                         UNION

#                         SELECT *
#                         FROM other_events
#                         ),
#                         stack_changes as (
#                         SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, player_id, player, 
#                         CASE WHEN type = 'Substitution' THEN 'Substitution - Off' ELSE type END AS type, position_name--, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                         FROM combine_sources
                        
                        

#                         UNION


#                         SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, substitution_replacement_id, substitution_replacement_name, 
#                         CASE 
#                         WHEN type = 'Substitution' THEN 'Substitution - On' 
#                         WHEN bad_behaviour_card IN ('Second Yellow', 'Red Card') THEN bad_behaviour_card
#                         ELSE type END AS type, position_name
#                         FROM combine_sources
#                         WHERE substitution_replacement_id IS NOT NULL OR bad_behaviour_card IN ('Second Yellow', 'Red Card')
#                         ),


#                         get_half as (
                                                
#                         SELECT distinct event_times.period, event_times.event_timestamp, event_times.match_id, team_id, player_id, player, type, position_name
#                         FROM (
#                               SELECT period, MAX(strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second)) event_timestamp, match_id, type
#                               FROM other_events
#                               WHERE type IN ('Half Start', 'Half End')
#                               GROUP BY  match_id, type, period
#                               ) event_times
#                         INNER JOIN (SELECT distinct match_id, team_id, player_id, player, event_timestamp, position_name, period
#                                        FROM stack_changes
#                                     ) all_players
#                               ON event_times.match_id = all_players.match_id
#                               AND event_times.period >= all_players.period
#                               AND event_times.event_timestamp >= all_players.event_timestamp
#                         WHERE (NOT (event_times.match_id = all_players.match_id AND event_times.period = all_players.period AND event_times.event_timestamp = all_players.event_timestamp))
#                         AND player_id IS NOT NULL
#                         ),


#                         id_lineup_change as (
#                         SELECT stack_changes.*,
#                         CASE 
#                         WHEN IFNULL(LAG(period,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),-1) != IFNULL(period,-1) THEN 1
#                         WHEN IFNULL(LAG(position_name,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),'-') != IFNULL(position_name,'-') THEN 1
#                         WHEN UPPER(IFNULL(type,'-')) LIKE '%SUBSTITUTION%' THEN 1
#                         WHEN UPPER(IFNULL(type,'-')) = 'BAD BEHAVIOUR' THEN 1
#                         WHEN IFNULL(LAG(position_name,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),'-') != IFNULL(position_name,'-') THEN 1
#                         ELSE 0
#                         END AS lineup_change
#                         FROM stack_changes
#                         WHERE player_id IS NOT NULL
#                         ),
#                         iso_changes as (
#                         SELECT distinct period, event_timestamp, match_id, team_id, player_id, player, type, position_name
#                         FROM id_lineup_change
#                         WHERE lineup_change = 1

#                         UNION

#                         SELECT distinct period, event_timestamp, match_id, team_id, player_id, player, type, position_name
#                         FROM get_half
                        
#                         ),
#                         find_end as (
#                         SELECT period, event_timestamp, 
#                         LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_timestamp,
#                         match_id, team_id, player_id, player, type, 
#                         LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type, position_name
#                         FROM iso_changes
#                         ),
#                         initial_intervals as (
#                         SELECT distinct period, event_timestamp interval_start, next_event_timestamp interval_end, match_id, team_id, player_id, player, type, next_event_type, position_name
#                         FROM find_end
#                         WHERE type NOT IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')
#                         AND player_id IS NOT NULL
#                         ),
#                         sub_out as (
#                         SELECT distinct player_id, event_timestamp, period, match_id, type
#                         FROM stack_changes
#                         WHERE type IN ('Substitution - Off', 'Second Yellow', 'Red Card', 'Bad Behaviour') AND player_id IS NOT NULL
#                         ),
#                         match_intervals_tmp as (
#                         SELECT --initial_intervals.*
#                         initial_intervals.period, initial_intervals.interval_start, 
#                         --CASE WHEN sub_out.event_timestamp < IFNULL(initial_intervals.interval_end, CURRENT_DATE)
#                         --THEN sub_out.event_timestamp
#                         --ELSE 
#                         initial_intervals.interval_end,
#                         --END AS interval_end, 
#                         initial_intervals.match_id, 
#                         initial_intervals.team_id, initial_intervals.player_id, initial_intervals.player, 
#                         --CASE WHEN sub_out.event_timestamp < IFNULL(initial_intervals.interval_end, CURRENT_DATE)
#                         --THEN sub_out.type
#                         --ELSE 
#                         initial_intervals.type,
#                         --END AS type, 
#                         initial_intervals.next_event_type, initial_intervals.position_name
#                         FROM initial_intervals
#                         ),
#                         match_intervals as (
#                               SELECT *
#                               FROM match_intervals_tmp
#                         WHERE type NOT IN ('Substitution - Off', 'Second Yellow', 'Red Card', 'Half End')                       
#                         ),
#                         get_player_position as (
#                         SELECT match_intervals.*, 
#                          CASE WHEN POSITION_SIDE IN ('RC','LC') THEN 'C' ELSE POSITION_SIDE END AS POSITION_SIDE_ADJ, 
#                          POSITION_TYPE_ALT, POSITION_BEHAVIOR, POSITION_TYPE, get_country.country_id
#                         FROM match_intervals
#                         LEFT JOIN read_parquet('{ADDITIONAL_DIR}/position_type.parquet') pt
#                               ON match_intervals.position_name = pt.position_name
#                         LEFT JOIN (SELECT distinct player_id, match_id, country_id FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')) get_country
#                            ON match_intervals.player_id = get_country.player_id
#                            AND match_intervals.match_id = get_country.match_id

#                          ),

#                          get_country_world_rank as (
#                          SELECT 
#                          lookup_id.*, conf.Confederation, 
#                          CASE 
#                          WHEN lookup_id.name LIKE '%Brazzaville%' THEN 134
#                          WHEN lookup_id.name LIKE '%Venezuela%' THEN 50
#                          WHEN lookup_id.name LIKE '%Korea%' AND lookup_id.name LIKE '%South%' THEN 22
#                          ELSE conf.World_Ranking_January_19_2026 
#                          END AS World_Ranking_January_19_2026
#                          FROM (SELECT id, name
#                                     FROM read_parquet('{STATSBOMB_DIR}/reference.parquet') 
#                                     WHERE table_name = 'country') lookup_id
#                         LEFT JOIN (SELECT *
#                                     FROM read_csv('{ADDITIONAL_DIR}/WC_Confederations.csv')) conf
#                            ON TRIM(lookup_id.name) = TRIM(conf.country)
#                         ),
#                          player_overlap as (
#                          SELECT match_id, period, team_id, interval_start, interval_end, --POSITION_TYPE, 
#                          SUM(World_Ranking_January_19_2026) AGG_WORLD_RANKING,
#                            SUM(CASE WHEN POSITION_TYPE = 'F' THEN 1 ELSE 0 END) AS FORWARDS,
#                            SUM(CASE WHEN POSITION_TYPE = 'M' THEN 1 ELSE 0 END) AS MIDFIELDERS,
#                            SUM(CASE WHEN POSITION_TYPE = 'B' THEN 1 ELSE 0 END) AS BACKS,
#                            SUM(CASE WHEN POSITION_TYPE = 'GK' THEN 1 ELSE 0 END) AS GOALKEEPER,

#                            SUM(CASE WHEN POSITION_TYPE = 'F' THEN World_Ranking_January_19_2026 ELSE 0 END) FORWARDS_WR,
#                            SUM(CASE WHEN POSITION_TYPE = 'M' THEN World_Ranking_January_19_2026 ELSE 0 END) AS MIDFIELDERS_WR,
#                            SUM(CASE WHEN POSITION_TYPE = 'B' THEN World_Ranking_January_19_2026 ELSE 0 END) AS BACKS_WR,
#                            SUM(CASE WHEN POSITION_TYPE = 'GK' THEN World_Ranking_January_19_2026 ELSE 0 END) AS GOALKEEPER_WR,

#                            FROM get_player_position
#                            LEFT JOIN get_country_world_rank
#                               ON get_player_position.country_id = id
#                            WHERE interval_end > interval_start
#                          GROUP BY match_id, period, team_id, interval_start, interval_end
#                          ),
#                          last_add_players as (
#                          SELECT match_checkpoints.match_id, match_checkpoints.period, match_checkpoints.team_id, match_checkpoints.interval_start, 
#                          SUM(AGG_WORLD_RANKING) AGG_WORLD_RANKING, 
#                          SUM(FORWARDS) FORWARDS, SUM(MIDFIELDERS) MIDFIELDERS, SUM(BACKS) BACKS, SUM(GOALKEEPER) GOALKEEPER,
#                          SUM(FORWARDS_WR) FORWARDS_WR, SUM(MIDFIELDERS_WR) MIDFIELDERS_WR, SUM(BACKS_WR) BACKS_WR, SUM(GOALKEEPER_WR) GOALKEEPER_WR
#                          FROM (
#                                  SELECT distinct match_id, period, team_id, interval_start
#                                  FROM player_overlap
#                                  ) match_checkpoints
#                         LEFT JOIN player_overlap
#                            ON match_checkpoints.match_id = player_overlap.match_id
#                            AND match_checkpoints.period = player_overlap.period
#                            AND match_checkpoints.team_id = player_overlap.team_id
#                            AND match_checkpoints.interval_start >= player_overlap.interval_start
#                            AND match_checkpoints.interval_start < player_overlap.interval_end
#                         GROUP BY match_checkpoints.match_id, match_checkpoints.period, match_checkpoints.team_id, match_checkpoints.interval_start
#                         ),
#                         final_intervals as (
#                         SELECT last_add_players.match_id, last_add_players.period, team_id, interval_start, 
#                         IFNULL(LEAD(interval_start,1) OVER (PARTITION BY last_add_players.match_id, last_add_players.period, team_id ORDER BY last_add_players.match_id, last_add_players.period, team_id, interval_start), half_end) interval_end,
#                         FORWARDS, MIDFIELDERS, BACKS, GOALKEEPER, AGG_WORLD_RANKING,
#                         FORWARDS_WR, MIDFIELDERS_WR, BACKS_WR, GOALKEEPER_WR
#                         FROM last_add_players
#                         LEFT JOIN (SELECT match_id, period, MAX(interval_end) half_end FROM player_overlap GROUP BY match_id, period) check_half_again
#                            ON last_add_players.match_id = check_half_again.match_id
#                            AND last_add_players.period = check_half_again.period
#                         --WHERE FORWARDS + MIDFIELDERS + BACKS + GOALKEEPER < 9
#                         )
#                            SELECT *
#                            FROM final_intervals
#                            WHERE FORWARDS + MIDFIELDERS + BACKS + GOALKEEPER < 11


#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)


# y_coords = duckdb.sql(f"""
#                         with starting_xi as (
#                          SELECT from_period period, 0 as minute, 0 as second, match_id, team_id, player_id, IFNULL(player_nickname, player_name) player, 'Starting XI' as type, position_name, 
#                       NULL as substitution_replacement_id, NULL as substitution_replacement_name, NULL as substitution_outcome, NULL as bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')
#                          WHERE from_period = 1 AND from_time = '00:00'
#                       ),
#                       other_events as (
#                          SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                          WHERE type IN (
#                                         'Half End',
#                                         'Substitution'--,
#                                         --'Tactical Shift'
#                                         )
#                             OR bad_behaviour_card IN ('Red Card', 'Second Yellow')
#                             OR (type = 'Half Start' AND period > 1)
#                         ),
#                         add_tactical as (
#                         SELECT tactical_shift.*
#                         FROM (
#                               SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                               FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                               WHERE type = 'Tactical Shift' ) tactical_shift

#                         LEFT JOIN other_events
#                            ON tactical_shift.period = other_events.period
#                            AND tactical_shift.minute = other_events.minute
#                            AND tactical_shift.second = other_events.second
#                            AND tactical_shift.team_id = other_events.team_id
#                            AND tactical_shift.player_id = other_events.player_id
#                         WHERE other_events.minute IS NULL
#                         ),
#                         combine_sources as (
#                         SELECT *
#                         FROM starting_xi

#                         UNION

#                         SELECT *
#                         FROM add_tactical
#                         ),
#                         stack_changes as (
#                         SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, player_id, player, 
#                         CASE WHEN type = 'Substitution' THEN 'Substitution - Off' ELSE type END AS type, position_name--, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                         FROM combine_sources
                        

#                         UNION


#                         SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, substitution_replacement_id, substitution_replacement_name, 
#                         CASE 
#                         WHEN type = 'Substitution' THEN 'Substitution - On' 
#                         WHEN bad_behaviour_card IN ('Second Yellow', 'Red Card') THEN bad_behaviour_card
#                         ELSE type END AS type, position_name
#                         FROM combine_sources
#                         WHERE substitution_replacement_id IS NOT NULL OR bad_behaviour_card IN ('Second Yellow', 'Red Card')
#                         ),


#                         get_half as (
                                                
#                         SELECT distinct event_times.period, event_times.event_timestamp, event_times.match_id, team_id, player_id, player, type, position_name
#                         FROM (
#                               SELECT period, MAX(strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second)) event_timestamp, match_id, type
#                               FROM other_events
#                               WHERE type IN ('Half Start', 'Half End')
#                               GROUP BY  match_id, type, period
#                               ) event_times
#                         INNER JOIN (SELECT distinct match_id, team_id, player_id, player, event_timestamp, position_name, period
#                                        FROM stack_changes
#                                     ) all_players
#                               ON event_times.match_id = all_players.match_id
#                               AND event_times.period >= all_players.period
#                               AND event_times.event_timestamp >= all_players.event_timestamp
#                         WHERE NOT (event_times.match_id = all_players.match_id AND event_times.period = all_players.period AND event_times.event_timestamp = all_players.event_timestamp)
#                         AND player_id IS NOT NULL
#                         ),


#                         id_lineup_change as (
#                         SELECT stack_changes.*,
#                         CASE 
#                         WHEN IFNULL(LAG(period,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),-1) != IFNULL(period,-1) THEN 1
#                         WHEN IFNULL(LAG(position_name,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),'-') != IFNULL(position_name,'-') THEN 1
#                         ELSE 0
#                         END AS lineup_change
#                         FROM stack_changes
#                         WHERE player_id IS NOT NULL
#                         ),
#                         iso_changes as (
#                         SELECT distinct period, event_timestamp, match_id, team_id, player_id, player, type, position_name
#                         FROM id_lineup_change
#                         WHERE lineup_change = 1

#                         UNION

#                         SELECT distinct period, event_timestamp, match_id, team_id, player_id, player, type, position_name
#                         FROM get_half
                        
#                         ),
#                         find_end as (
#                         SELECT period, event_timestamp, 
#                         LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_timestamp,
#                         match_id, team_id, player_id, player, type, 
#                         LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type, position_name
#                         FROM iso_changes
#                         ),
#                         initial_intervals as (
#                         SELECT distinct period, event_timestamp interval_start, next_event_timestamp interval_end, match_id, team_id, player_id, player, type, next_event_type, position_name
#                         FROM find_end
#                         WHERE type NOT IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')
#                         AND player_id IS NOT NULL
#                         ),
#                         sub_out as (
#                         SELECT distinct player_id, event_timestamp, period, match_id, type
#                         FROM stack_changes
#                         WHERE type IN ('Substitution - Off', 'Second Yellow', 'Red Card', 'Bad Behaviour') AND player_id IS NOT NULL
#                         ),
#                         match_intervals_tmp as (
#                         SELECT --initial_intervals.*
#                         initial_intervals.period, initial_intervals.interval_start, 
#                         CASE WHEN sub_out.event_timestamp < IFNULL(initial_intervals.interval_end, CURRENT_DATE)
#                         THEN sub_out.event_timestamp
#                         ELSE initial_intervals.interval_end
#                         END AS interval_end, 
#                         initial_intervals.match_id, 
#                         initial_intervals.team_id, initial_intervals.player_id, initial_intervals.player, 
#                         CASE WHEN sub_out.event_timestamp < IFNULL(initial_intervals.interval_end, CURRENT_DATE)
#                         THEN sub_out.type
#                         ELSE initial_intervals.type
#                         END AS type, initial_intervals.next_event_type, initial_intervals.position_name
#                         FROM initial_intervals
#                         LEFT JOIN sub_out
#                            ON initial_intervals.match_id = sub_out.match_id
#                            AND initial_intervals.player_id = sub_out.player_id
#                         WHERE (initial_intervals.period < IFNULL(sub_out.period,9999999)
#                            OR (initial_intervals.period = IFNULL(sub_out.period,9999999) 
#                                  AND initial_intervals.interval_start < sub_out.event_timestamp
#                                  )
#                            )
#                            --AND initial_intervals.match_id = 7581
#                            --initial_intervals.match_id = 7298
#                         ),
#                         non_player_event_lineup_changes as (
#                         SELECT period, match_id, team_id, interval_end event_timestamp, 'Team Substition/Dismissal' AS type
#                         FROM match_intervals_tmp
#                         WHERE UPPER(next_event_type) LIKE '%SUBSTITUTION%' OR next_event_type IN ('Second Yellow', 'Red Card')
#                         ),
#                         check_other_subs as (

#                         SELECT first_check.period, first_check.match_id, first_check.team_id, first_check.event_timestamp, first_check.type, position_name, first_check.player_id, player
#                         FROM (SELECT distinct match_intervals_tmp.period, match_intervals_tmp.match_id, match_intervals_tmp.team_id, non_player_event_lineup_changes.event_timestamp, non_player_event_lineup_changes.type, position_name, match_intervals_tmp.player_id, player
#                               FROM match_intervals_tmp
#                               INNER JOIN non_player_event_lineup_changes
#                                  ON match_intervals_tmp.period = non_player_event_lineup_changes.period
#                                  AND match_intervals_tmp.match_id = non_player_event_lineup_changes.match_id
#                                  AND match_intervals_tmp.team_id = non_player_event_lineup_changes.team_id
#                                  AND non_player_event_lineup_changes.event_timestamp >= match_intervals_tmp.interval_start
#                                  AND non_player_event_lineup_changes.event_timestamp < match_intervals_tmp.interval_end
#                               ) first_check
#                         LEFT JOIN (SELECT period, match_id, team_id, player_id, interval_start FROM match_intervals_tmp) check_other
#                            ON first_check.period = check_other.period
#                            AND first_check.match_id = check_other.match_id
#                            AND first_check.team_id = check_other.team_id
#                            AND first_check.player_id = check_other.player_id
#                            AND first_check.event_timestamp = interval_start
#                         WHERE check_other.player_id IS NULL

#                         UNION

#                         SELECT distinct period, match_id, team_id, interval_start, type, position_name, player_id, player
#                         FROM match_intervals_tmp

#                         UNION

#                         SELECT distinct period, match_id, team_id, interval_end, next_event_type, position_name, player_id, player
#                         FROM match_intervals_tmp

#                         ),
#                         match_intervals as (
#                               SELECT *
#                               FROM (
#                                     SELECT period, match_id, team_id, position_name, player_id, player, event_timestamp interval_start,
#                                     LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) interval_end, type,
#                                     LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type,

#                                     FROM check_other_subs
#                                     )
#                         WHERE type NOT IN ('Substitution - Off', 'Second Yellow', 'Red Card', 'Half End')                       
#                         ),

#                          get_country_world_rank as (
#                          SELECT 
#                          lookup_id.*, conf.Confederation, 
#                          CASE 
#                          WHEN lookup_id.name LIKE '%Brazzaville%' THEN 134
#                          WHEN lookup_id.name LIKE '%Venezuela%' THEN 50
#                          WHEN lookup_id.name LIKE '%Korea%' AND lookup_id.name LIKE '%South%' THEN 22
#                          ELSE conf.World_Ranking_January_19_2026 
#                          END AS World_Ranking_January_19_2026
#                          FROM (SELECT id, name
#                                     FROM read_parquet('{STATSBOMB_DIR}/reference.parquet') 
#                                     WHERE table_name = 'country') lookup_id
#                         LEFT JOIN (SELECT *
#                                     FROM read_csv('{ADDITIONAL_DIR}/WC_Confederations.csv')) conf
#                            ON TRIM(lookup_id.name) = TRIM(conf.country)
#                         ),
#                         find_country_quartile as (
#                         SELECT distinct Confederation, id country_id,
#                         World_Ranking_January_19_2026--,
#                         --CASE 
#                         --WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.25 THEN 'Q1'
#                         --WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.5 THEN 'Q2'
#                         --WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.75 THEN 'Q3'
#                         --ELSE 'Q4'
#                         --END AS COUNTRY_QUARTILE
#                         FROM get_country_world_rank
#                         ),
#                         get_player_position as (
#                         SELECT distinct match_intervals.*, 
#                          CASE WHEN POSITION_SIDE IN ('RC','LC') THEN 'C' ELSE POSITION_SIDE END AS POSITION_SIDE_ADJ, 
#                          POSITION_TYPE_ALT, POSITION_BEHAVIOR, POSITION_TYPE, get_country.country_id, World_Ranking_January_19_2026, --COUNTRY_QUARTILE, 
#                          Confederation
#                         FROM match_intervals
#                         LEFT JOIN read_parquet('{ADDITIONAL_DIR}/position_type.parquet') pt
#                               ON match_intervals.position_name = pt.position_name
#                         LEFT JOIN (SELECT distinct player_id, match_id, country_id FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')) get_country
#                            ON match_intervals.player_id = get_country.player_id
#                            AND match_intervals.match_id = get_country.match_id
#                         LEFT JOIN find_country_quartile
#                            ON get_country.country_id = find_country_quartile.country_id

#                          ),
#                         get_ranks as (
#                         SELECT team_id, match_id, period, interval_start, interval_end, player_id, --country_id, --POSITION_SIDE_ADJ, POSITION_TYPE_ALT, POSITION_BEHAVIOR, 
#                         POSITION_TYPE, World_Ranking_January_19_2026, --COUNTRY_QUARTILE, 
#                         Confederation
#                         --,
#                         --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ ORDER BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ, player_id) PLAYER_POSITION_SIDE_ADJ_ID_RANK,
#                         --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE, player_id) PLAYER_POSITION_TYPE_ID_RANK,
#                         --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT, player_id) PLAYER_POSITION_TYPE_ALT_ID_RANK,
#                         --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, country_id ORDER BY match_id, team_id, period, interval_start, country_id, player_id) PLAYER_COUNTRY_ID_RANK,
#                         --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, interval_end ORDER BY match_id, team_id, period, interval_start, interval_end, player_id) PLAYER_SQUAD_RANK
                        
#                         FROM get_player_position
#                         WHERE interval_start != interval_end
#                         ),
#                         apply_attr as (
                         
#                          SELECT match_id, period, team_id, interval_start, interval_end, --POSITION_TYPE, 
#                          CASE WHEN POSITION_TYPE = 'F' THEN 1 ELSE 0 END AS FORWARDS,
#                          CASE WHEN POSITION_TYPE = 'M' THEN 1 ELSE 0 END AS MIDFIELDERS,
#                          CASE WHEN POSITION_TYPE = 'B' THEN 1 ELSE 0 END AS BACKS,
#                          CASE WHEN POSITION_TYPE = 'GK' THEN 1 ELSE 0 END AS GOALKEEPER,
#                          World_Ranking_January_19_2026, 

#                          CASE WHEN mc.cluster = 0 THEN 1 ELSE 0 END AS C0,
#                          CASE WHEN mc.cluster = 1 THEN 1 ELSE 0 END AS C1,
#                          CASE WHEN mc.cluster = 2 THEN 1 ELSE 0 END AS C2,
#                          CASE WHEN mc.cluster = 3 THEN 1 ELSE 0 END AS C3,
#                          CASE WHEN mc.cluster = 4 THEN 1 ELSE 0 END AS C4,
                         
#                          --CASE WHEN IFNULL(mc.cluster, wc.cluster) = 0 THEN 1 ELSE 0 END AS C0,
#                          --CASE WHEN IFNULL(mc.cluster, wc.cluster) = 1 THEN 1 ELSE 0 END AS C1,
#                          --CASE WHEN IFNULL(mc.cluster, wc.cluster) = 2 THEN 1 ELSE 0 END AS C2,
#                          --CASE WHEN IFNULL(mc.cluster, wc.cluster) = 3 THEN 1 ELSE 0 END AS C3,
#                          --CASE WHEN IFNULL(mc.cluster, wc.cluster) = 4 THEN 1 ELSE 0 END AS C4,
#                          CASE 
#                          WHEN mc.cluster IS NOT NULL THEN 'M'
#                          --WHEN wc.cluster IS NOT NULL THEN 'W'
#                          ELSE NULL
#                          END AS MEN_WOMEN, --, 
#                          CASE WHEN Confederation = 'UEFA' THEN 1 ELSE 0 END AS UEFA,
#                          CASE WHEN Confederation = 'AFC' THEN 1 ELSE 0 END AS AFC,
#                          CASE WHEN Confederation = 'OFC' THEN 1 ELSE 0 END AS OFC,
#                          CASE WHEN Confederation = 'COMNEBOL' THEN 1 ELSE 0 END AS COMNEBOL,
#                          CASE WHEN Confederation = 'CONCACAF' THEN 1 ELSE 0 END AS CONCACAF,
#                          CASE WHEN Confederation = 'CAF' THEN 1 ELSE 0 END AS CAF

#                          FROM get_ranks
#                          LEFT JOIN (SELECT player_id, cluster
#                                        FROM read_parquet('{ANALYSIS_DIR}/Mens_Clustering.parquet')) mc
#                               On get_ranks.player_id = mc.player_id
#                         --LEFT JOIN (SELECT player_id, cluster
#                                        --FROM read_parquet('{ANALYSIS_DIR}/Womens_Clustering.parquet')) wc
#                               --On get_ranks.player_id = wc.player_id
#                      ),

#                      check_totals as (

#                      SELECT match_id, period, team_id, interval_start, interval_end, MEN_WOMEN, --COUNTRY_QUARTILE, Confederation,
#                      SUM(FORWARDS) FORWARDS, SUM(MIDFIELDERS) MIDFIELDERS, SUM(BACKS) BACKS, SUM(GOALKEEPER) GOALKEEPER,
#                      SUM(C0) C0, SUM(C1) C1, SUM(C2) C2, SUM(C3) C3, SUM(C4) C4, 
#                      --SUM(UEFA) UEFA, SUM(AFC) AFC, SUM(OFC) OFC, SUM(COMNEBOL) COMNEBOL, SUM(CONCACAF) CONCACAF, SUM(CAF) CAF,
#                      AVG(World_Ranking_January_19_2026) AVERAGE_WR
#                      FROM apply_attr
#                      GROUP BY match_id, period, team_id, interval_start, interval_end, MEN_WOMEN, --COUNTRY_QUARTILE, Confederation
                     
#                      )
#                      SELECT *
#                      FROM check_totals
#                      WHERE FORWARDS + MIDFIELDERS + BACKS < 8

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT *
#                       FROM (
#                       SELECT home_team_id team_id, home_managers managers, match_id
#                       FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

#                       UNION

#                       SELECT away_team_id team_id, away_managers managers, match_id
#                       FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
#                       )

#                       WHERE UPPER(managers) LIKE '%CANN%'



#                      """)#.write_csv('all_countries.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT distinct player_id, country_name, team_name, player_name, player_nickname--, position_name
#                       FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')
#                       WHERE UPPER(country_name) LIKE '%UZB%'
#                       AND UPPER(team_name) NOT LIKE '%WOMEN%'
#                       ORDER BY player_name



#                      """)#.write_csv('specific_country.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT distinct player_id, country_name, team_name, player_name, player_nickname--, position_name
#                       FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')
#                       WHERE --(
#                       UPPER(country_name) LIKE '%KOREA%'
#                       --IN 
#                       --(--'Algeria',
#                        --'Argentina',
#                        -- 'Australia',
#                         --'Austria',
#                        -- 'Belgium',
#                        -- 'Brazil',
#                        -- 'Canada',
#                         --'Cape Verde'--,
#                        -- 'Colombia',
#                        -- 'Croatia',
#                        -- 'Curaçao',
#                        -- 'Ecuador',
#                        -- 'Egypt',
#                         --'England',
#                        -- 'France',
#                        -- 'Germany',
#                        -- 'Ghana',
#                        -- 'Haiti',
#                        -- 'Congo, (Kinshasa)',
#                        -- 'Iraq',
#                        -- 'Iran, Islamic Republic of',
#                         --"Côte d'Ivoire",
#                         --'Japan',
#                        -- 'Jordan',
#                        -- 'Korea (South)',
#                        -- 'Mexico',
#                        -- 'Morocco',
#                        -- 'Netherlands',
#                         --'New Zealand',
#                        -- 'Norway',
#                        -- 'Panama',
#                        -- 'Paraguay',
#                       --  'Portugal',
#                       --  'Qatar',
#                        -- 'Saudi Arabia',
#                        -- 'Scotland',
#                        -- 'Senegal',
#                        -- 'South Africa',
#                        -- 'Spain',
#                         --'Switzerland',
#                         --'Tunisia',
#                         --'Bosnia and Herzegovina',
#                         --'Sweden',
#                         --'Turkey',
#                         --'Czech Republic',
#                         'United States of America'--,
#                         --'Uruguay',
#                         --'Uzbekistan'
#                         --
#                         --)
#                                 OR team_id IN (
#                                         SELECT distinct id
#                                         FROM (
#                                               SELECT distinct id, name
#                                               FROM read_parquet('{STATSBOMB_DIR}/reference.parquet')
#                                               WHERE table_name = 'team' AND extra_info = 'male'
#                                               AND id IN (
#                                                         SELECT distinct away_team_id
#                                                         FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
#                                                         WHERE is_international = true

#                                                         UNION

#                                                         SELECT distinct home_team_id
#                                                         FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
#                                                         WHERE is_international = true
#                                                         )
#                                         )
#                                       WHERE UPPER(name) LIKE '%KOREA%'
#                                       --IN (
#                                       --'Algeria',
#                        --'Argentina',
#                         --'Australia',
#                         --'Austria',
#                         --'Belgium',
#                         --'Brazil',
#                         --'Canada',
#                         --'Cape Verde Islands'--,
#                         --'Colombia',
#                        -- 'Croatia',
#                        -- 'Curaçao',
#                         --'Ecuador',
#                        -- 'Egypt',
#                        -- 'England',
#                        -- 'France',
#                         --'Germany',
#                         --'Ghana',
#                         --'Haiti',
#                         --'Congo, (Kinshasa)',
#                         --'Iraq',
#                         --'Iran, Islamic Republic of',
#                         ----"Côte d'Ivoire",
#                         --'Japan',
#                         --'Jordan',
#                         --'Korea (South)',
#                         --'Mexico',
#                         --'Morocco',
#                         --'Netherlands',
#                         --'New Zealand',
#                         --'Norway',
#                         --'Panama',
#                         --'Paraguay',
#                         --'Portugal',
#                         --'Qatar',
#                         --'Saudi Arabia',
#                         --'Scotland',
#                         --'Senegal',
#                         --'South Africa',
#                         --'Spain',
#                         --'Switzerland',
#                         --'Tunisia',
#                         --'Bosnia and Herzegovina',
#                         --'Sweden',
#                         --'Turkey',
#                         --'Czech Republic',
#                         'United States'--,
#                         --'Uruguay',
#                         --'Uzbekistan'
#                         --)
#                                       --)
#                               ) AND UPPER(team_name) NOT LIKE '%WOMEN%' AND UPPER(team_name) NOT LIKE '%WFC%'
#                       ORDER BY player_name




#                      """)#.write_csv('check_all_players_US.csv')

# print(y_coords)



# y_coords = duckdb.sql(f"""

                      
#                       SELECT match_id, OFF_TEAM_ID, DEF_TEAM_ID, POSSESSION, period, COUNT(*)
#                       FROM read_parquet('{ADDITIONAL_DIR}/for_regression.parquet') 
#                       GROUP BY match_id, OFF_TEAM_ID, DEF_TEAM_ID, POSSESSION, period
#                       HAVING COUNT (*) > 1


#                      """)#.write_csv('player_country_cluster.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       with event_data as (
#                       SELECT match_id, period, possession, possession_team_id, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp
#                       FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                      
#                       ),
#                       add_groups_possession as (
#                       SELECT distinct event_data.match_id, event_data.period, event_data.possession, off_lineup.PLAYERS_ON_PITCH OFF_PLAYERS_ON_PITCH,
#                       off_lineup.MEN_WOMEN OFF_MEN_WOMEN, off_lineup.GROUPING_PK OFF_GROUPING_PK, def_lineup.GROUPING_PK DEF_GROUPING_PK, total_xg, possession_type, n_shots, duration_seconds,

#                       off_lineup.BACKS OFF_BACKS, off_lineup.MIDFIELDERS OFF_MIDFIELDERS, off_lineup.FORWARDS OFF_FORWARDS, off_lineup.GOALKEEPER OFF_GOALKEEPER, 
#                       off_lineup.C0 OFF_C0, off_lineup.C1 OFF_C1, off_lineup.C2 OFF_C2, off_lineup.C3 OFF_C3, off_lineup.C4 OFF_C4, off_lineup.C5 OFF_C5,

#                       def_lineup.MEN_WOMEN DEF_MEN_WOMEN, def_lineup.PLAYERS_ON_PITCH DEF_PLAYERS_ON_PITCH, 
#                       def_lineup.BACKS DEF_BACKS, def_lineup.MIDFIELDERS DEF_MIDFIELDERS, def_lineup.FORWARDS DEF_FORWARDS, def_lineup.GOALKEEPER DEF_GOALKEEPER, 
#                       def_lineup.C0 DEF_C0, def_lineup.C1 DEF_C1, def_lineup.C2 DEF_C2, def_lineup.C3 DEF_C3, def_lineup.C4 DEF_C4, def_lineup.C5 DEF_C5
                      

#                       FROM event_data
#                       LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') off_lineup
#                         ON event_data.match_id = off_lineup.match_id
#                         AND event_data.period = off_lineup.period
#                         AND event_data.possession_team_id = off_lineup.team_id
#                         AND event_timestamp >= off_lineup.interval_start
#                         AND event_timestamp < IFNULL(off_lineup.interval_end, CURRENT_TIMESTAMP)
#                       LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') def_lineup
#                         ON event_data.match_id = def_lineup.match_id
#                         AND event_data.period = def_lineup.period
#                         AND event_data.possession_team_id != def_lineup.team_id
#                         AND event_timestamp >= def_lineup.interval_start
#                         AND event_timestamp < IFNULL(def_lineup.interval_end, CURRENT_TIMESTAMP)
#                       LEFT JOIN read_parquet('{ADDITIONAL_DIR}/possession_types.parquet') pt
#                         ON event_data.match_id = pt.match_id
#                         AND event_data.period = pt.period
#                         AND event_data.possession = pt.possession
#                      ),
#                      agg_all as (
#                      SELECT possession_type, 
#                      OFF_MEN_WOMEN, OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4, OFF_C5,
#                      DEF_MEN_WOMEN, DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4, DEF_C5, 
#                      OFF_PLAYERS_ON_PITCH, DEF_PLAYERS_ON_PITCH,
#                      SUM(total_xg) total_xg, SUM(n_shots) n_shots, SUM(duration_seconds) / 60 duration_minutes
#                      FROM add_groups_possession
#                      WHERE OFF_GROUPING_PK IS NOT NULL AND possession_type IS NOT NULL
#                      GROUP BY possession_type, 
#                      OFF_MEN_WOMEN, OFF_BACKS, OFF_MIDFIELDERS, OFF_FORWARDS, OFF_GOALKEEPER, OFF_C0, OFF_C1, OFF_C2, OFF_C3, OFF_C4, OFF_C5, OFF_PLAYERS_ON_PITCH,
#                      DEF_MEN_WOMEN, DEF_BACKS, DEF_MIDFIELDERS, DEF_FORWARDS, DEF_GOALKEEPER, DEF_C0, DEF_C1, DEF_C2, DEF_C3, DEF_C4, DEF_C5, DEF_PLAYERS_ON_PITCH
#                      )
#                      SELECT *
#                      FROM agg_all
#                      WHERE OFF_MEN_WOMEN = 'M'


#                      """)#.write_csv('player_country_cluster.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       with check_position as (
#                       SELECT player_id, POSITION_TYPE, COUNT(distinct match_id) matches
#                       FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet') 
#                       GROUP BY player_id, POSITION_TYPE
#                       ),
#                       player_position as (
#                       SELECT cp.*
#                       FROM check_position cp
#                       INNER JOIN (
#                       SELECT player_id, MAX(matches) most_freq
#                       FROM check_position
#                       GROUP BY player_id) get_max
#                         ON cp.player_id = get_max.player_id
#                         AND most_freq = matches
#                      )
#                      SELECT l.country_name, gender, l.player_id
#                       FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet') l
#                       LEFT JOIN read_parquet('{STATSBOMB_DIR}/matches.parquet') m
#                         ON l.match_id = m.match_id
                     
                     


#                      """)#.write_csv('player_country_cluster.csv')

# print(y_coords)


# y_coords = duckdb.sql(f"""

#                       SELECT type, period, timestamp, minute, second
#                       FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
#                       WHERE match_id = 3869685 AND possession = 196


#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                      with starting_xi as (
#                          SELECT from_period period, 0 as minute, 0 as second, match_id, team_id, player_id, IFNULL(player_nickname, player_name) player, 'Starting XI' as type, position_name, 
#                       NULL as substitution_replacement_id, NULL as substitution_replacement_name, NULL as substitution_outcome, NULL as bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')
#                          WHERE from_period = 1 AND from_time = '00:00'
#                       ),
#                       other_events as (
#                          SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                          FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                          WHERE type IN (
#                                         'Half End',
#                                         'Substitution'--,
#                                         --'Tactical Shift'
#                                         )
#                             OR bad_behaviour_card IN ('Red Card', 'Second Yellow')
#                             OR (type = 'Half Start' AND period > 1)
#                         ),
#                         add_tactical as (
#                         SELECT tactical_shift.*
#                         FROM (
#                               SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                               FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
#                               WHERE type = 'Tactical Shift' ) tactical_shift

#                         LEFT JOIN other_events
#                            ON tactical_shift.period = other_events.period
#                            AND tactical_shift.minute = other_events.minute
#                            AND tactical_shift.second = other_events.second
#                            AND tactical_shift.team_id = other_events.team_id
#                            AND tactical_shift.player_id = other_events.player_id
#                         WHERE other_events.minute IS NULL
#                         ),
#                         combine_sources as (
#                         SELECT *
#                         FROM starting_xi

#                         UNION

#                         SELECT *
#                         FROM add_tactical

#                         UNION

#                         SELECT *
#                         FROM other_events
#                         ),
#                         stack_changes as (
#                         SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, player_id, player, 
#                         CASE WHEN type = 'Substitution' THEN 'Substitution - Off' ELSE type END AS type, position_name--, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
#                         FROM combine_sources
                        
                        

#                         UNION


#                         SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, substitution_replacement_id, substitution_replacement_name, 
#                         CASE 
#                         WHEN type = 'Substitution' THEN 'Substitution - On' 
#                         WHEN bad_behaviour_card IN ('Second Yellow', 'Red Card') THEN bad_behaviour_card
#                         ELSE type END AS type, position_name
#                         FROM combine_sources
#                         WHERE substitution_replacement_id IS NOT NULL OR bad_behaviour_card IN ('Second Yellow', 'Red Card')
#                         ),


#                         get_half as (
                                                
#                         SELECT distinct event_times.period, event_times.event_timestamp, event_times.match_id, all_players.team_id, all_players.player_id, event_times.type, position_name, period_removed
#                         FROM (
#                               SELECT period, MAX(strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second)) event_timestamp, match_id, type
#                               FROM other_events
#                               WHERE type IN ('Half Start', 'Half End')
#                               GROUP BY  match_id, type, period
#                               ) event_times
#                         INNER JOIN (SELECT distinct match_id, team_id, player_id, event_timestamp, position_name, period, type
#                                        FROM stack_changes
                                       
#                                     ) all_players
#                               ON event_times.match_id = all_players.match_id
#                               AND event_times.period >= all_players.period
#                               AND event_times.event_timestamp >= all_players.event_timestamp
#                         LEFT JOIN (SELECT match_id, team_id, player_id, period period_removed, event_timestamp timestamp_removed FROM stack_changes WHERE type IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')) check_removal
#                           ON event_times.match_id = check_removal.match_id
#                           AND all_players.team_id = check_removal.team_id
#                           AND all_players.player_id = check_removal.player_id
#                         WHERE (NOT (event_times.match_id = all_players.match_id AND event_times.period = all_players.period AND event_times.event_timestamp = all_players.event_timestamp))
#                         AND all_players.player_id IS NOT NULL
#                         AND (
#                               (event_times.type = 'Half Start' AND event_times.period < IFNULL(period_removed,9999)) 
#                               OR 
#                               (event_times.type = 'Half Start' AND event_times.period = IFNULL(period_removed,9999) AND event_times.event_timestamp < IFNULL(timestamp_removed,CURRENT_TIMESTAMP))
#                               OR
#                               (event_times.type = 'Half End' AND event_times.period < IFNULL(period_removed,9999))
#                               )
#                         ),


#                         id_lineup_change as (
#                         SELECT stack_changes.*,
#                         CASE 
#                         WHEN IFNULL(LAG(period,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),-1) != IFNULL(period,-1) THEN 1
#                         WHEN IFNULL(LAG(position_name,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),'-') != IFNULL(position_name,'-') THEN 1
#                         WHEN IFNULL(LAG(player_id,1) OVER (PARTITION BY match_id, team_id, position_name, period ORDER BY match_id, team_id, position_name, period, event_timestamp),-1) != IFNULL(player_id,-1) THEN 1
#                         ELSE 0
#                         END AS lineup_change
#                         FROM stack_changes
#                         WHERE player_id IS NOT NULL
#                         ),
#                         iso_changes as (
#                         SELECT distinct period, event_timestamp, match_id, team_id, player_id, type, position_name
#                         FROM id_lineup_change
#                         WHERE lineup_change = 1

#                         UNION

#                         SELECT distinct period, event_timestamp, match_id, team_id, player_id, type, position_name
#                         FROM get_half
                        
#                         ),
#                         find_end as (
#                         SELECT period, event_timestamp, 
#                         LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_timestamp,
#                         match_id, team_id, player_id, type, 
#                         LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type, position_name
#                         FROM iso_changes
#                         ),
#                         initial_intervals as (
#                         SELECT distinct period, event_timestamp interval_start, next_event_timestamp interval_end, match_id, team_id, player_id, type, next_event_type, position_name
#                         FROM find_end
#                         WHERE type NOT IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')
#                         AND player_id IS NOT NULL
#                         ),
#                         apply_other_attr as (
#                         SELECT distinct get_all_players.match_id, 
#                         get_all_players.team_id, 
#                         get_all_players.period, 
#                         get_all_players.interval_start, 
#                         get_all_players.interval_end, 
#                         get_all_players.player_id, 
#                         POSITION_TYPE, 
#                         CASE WHEN mc.cluster IS NOT NULL THEN 'M' ELSE 'W' END AS MEN_WOMEN,
#                         IFNULL(wc.cluster, mc.cluster) player_cluster
#                         FROM (
#                               SELECT distinct team_checkpoints.*, player_id, position_name
#                               FROM (

#                                     SELECT distinct_times.*, IFNULL(LEAD(interval_start,1) OVER (PARTITION BY distinct_times.match_id, team_id, distinct_times.period ORDER BY distinct_times.match_id, team_id, distinct_times.period, interval_start), max_half_end) interval_end
#                                     FROM (
#                                           SELECT distinct match_id, team_id, period, interval_start
#                                           FROM initial_intervals
#                                           ) distinct_times
#                                        LEFT JOIN (SELECT match_id, period, MAX(interval_end) max_half_end FROM initial_intervals GROUP BY match_id, period) check_half
#                                           ON distinct_times.match_id = check_half.match_id
#                                           AND distinct_times.period = check_half.period
#                                     ) team_checkpoints
#                               LEFT JOIN initial_intervals
#                                  ON team_checkpoints.match_id = initial_intervals.match_id
#                                  AND team_checkpoints.team_id = initial_intervals.team_id
#                                  AND team_checkpoints.period = initial_intervals.period
#                                  AND team_checkpoints.interval_start >= initial_intervals.interval_start
#                                  AND team_checkpoints.interval_start < initial_intervals.interval_end
#                               ) get_all_players 
#                         LEFT JOIN read_parquet('{ADDITIONAL_DIR}/position_type.parquet') pt
#                            ON get_all_players.position_name = pt.position_name
#                         LEFT JOIN read_parquet('{ADDITIONAL_DIR}/Mens_Clustering.parquet') mc
#                            ON get_all_players.player_id = mc.player_id
#                         LEFT JOIN read_parquet('{ADDITIONAL_DIR}/Womens_Clustering.parquet') wc
#                            ON get_all_players.player_id = wc.player_id
#                         WHERE interval_start < interval_end
#                         ),
#                         agg_attributes as (
#                         SELECT match_id, team_id, period, interval_start, interval_end, MEN_WOMEN,
#                         SUM(CASE WHEN POSITION_TYPE = 'B' THEN 1 ELSE 0 END) AS BACKS,
#                         SUM(CASE WHEN POSITION_TYPE = 'M' THEN 1 ELSE 0 END) AS MIDFIELDERS,
#                         SUM(CASE WHEN POSITION_TYPE = 'F' THEN 1 ELSE 0 END) AS FORWARDS,
#                         SUM(CASE WHEN POSITION_TYPE = 'GK' THEN 1 ELSE 0 END) AS GOALKEEPER,

#                         SUM(CASE WHEN player_cluster = 0 THEN 1 ELSE 0 END) AS C0,
#                         SUM(CASE WHEN player_cluster = 1 THEN 1 ELSE 0 END) AS C1,
#                         SUM(CASE WHEN player_cluster = 2 THEN 1 ELSE 0 END) AS C2,
#                         SUM(CASE WHEN player_cluster = 3 THEN 1 ELSE 0 END) AS C3,
#                         SUM(CASE WHEN player_cluster = 4 THEN 1 ELSE 0 END) AS C4,
#                         SUM(CASE WHEN player_cluster = 5 THEN 1 ELSE 0 END) AS C5,
#                         COUNT(*) PLAYERS_ON_PITCH
#                         FROM apply_other_attr

#                         GROUP BY match_id, team_id, period, interval_start, interval_end, MEN_WOMEN
#                         )
#                         SELECT *
#                         FROM agg_attributes
#                         WHERE match_id = 3900519 AND team_id = 146 AND period = 2
#                         --AND player_id NOT IN (2946, 3150, 3184, 3196, 3256)
#                         --AND type != 'Half Start'
#                         --ORDER BY player_id, interval_start


#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT match_id, possession, possession_team_id, possession_type
#                       FROM read_parquet('{ADDITIONAL_DIR}/possession_types.parquet')

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)