import duckdb
from pathlib import Path


EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "team_formation_timeline.parquet")



# y_coords = duckdb.sql(f"""

#                       SELECT *
#                       FROM read_parquet('{ANALYSIS_DIR}/Womens_Clustering.parquet')
#                       WHERE player_id = 4651

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


y_coords = duckdb.sql(f"""
                        with starting_xi as (
                         SELECT from_period period, 0 as minute, 0 as second, match_id, team_id, player_id, IFNULL(player_nickname, player_name) player, 'Starting XI' as type, position_name, 
                      NULL as substitution_replacement_id, NULL as substitution_replacement_name, NULL as substitution_outcome, NULL as bad_behaviour_card
                         FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')
                         WHERE from_period = 1 AND from_time = '00:00'
                      ),
                      other_events as (
                         SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
                         FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
                         WHERE type IN (
                                        'Half End',
                                        'Substitution'--,
                                        --'Tactical Shift'
                                        )
                            OR bad_behaviour_card IN ('Red Card', 'Second Yellow')
                            OR (type = 'Half Start' AND period > 1)
                        ),
                        add_tactical as (
                        SELECT tactical_shift.*
                        FROM (
                              SELECT period, minute, second, match_id, team_id, player_id, player, type, position, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
                              FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
                              WHERE type = 'Tactical Shift' ) tactical_shift

                        LEFT JOIN other_events
                           ON tactical_shift.period = other_events.period
                           AND tactical_shift.minute = other_events.minute
                           AND tactical_shift.second = other_events.second
                           AND tactical_shift.team_id = other_events.team_id
                           AND tactical_shift.player_id = other_events.player_id
                        WHERE other_events.minute IS NULL
                        ),
                        combine_sources as (
                        SELECT *
                        FROM starting_xi

                        UNION

                        SELECT *
                        FROM add_tactical
                        ),
                        stack_changes as (
                        SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, player_id, player, 
                        CASE WHEN type = 'Substitution' THEN 'Substitution - Off' ELSE type END AS type, position_name--, substitution_replacement_id, substitution_replacement_name, substitution_outcome, bad_behaviour_card
                        FROM combine_sources
                        

                        UNION


                        SELECT period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, match_id, team_id, substitution_replacement_id, substitution_replacement_name, 
                        CASE 
                        WHEN type = 'Substitution' THEN 'Substitution - On' 
                        WHEN bad_behaviour_card IN ('Second Yellow', 'Red Card') THEN bad_behaviour_card
                        ELSE type END AS type, position_name
                        FROM combine_sources
                        WHERE substitution_replacement_id IS NOT NULL OR bad_behaviour_card IN ('Second Yellow', 'Red Card')
                        ),


                        get_half as (
                                                
                        SELECT distinct event_times.period, event_times.event_timestamp, event_times.match_id, team_id, player_id, player, type, position_name
                        FROM (
                              SELECT period, MAX(strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second)) event_timestamp, match_id, type
                              FROM other_events
                              WHERE type IN ('Half Start', 'Half End')
                              GROUP BY  match_id, type, period
                              ) event_times
                        INNER JOIN (SELECT distinct match_id, team_id, player_id, player, event_timestamp, position_name, period
                                       FROM stack_changes
                                    ) all_players
                              ON event_times.match_id = all_players.match_id
                              AND event_times.period >= all_players.period
                              AND event_times.event_timestamp >= all_players.event_timestamp
                        WHERE NOT (event_times.match_id = all_players.match_id AND event_times.period = all_players.period AND event_times.event_timestamp = all_players.event_timestamp)
                        AND player_id IS NOT NULL
                        ),


                        id_lineup_change as (
                        SELECT stack_changes.*,
                        CASE 
                        WHEN IFNULL(LAG(period,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),-1) != IFNULL(period,-1) THEN 1
                        WHEN IFNULL(LAG(position_name,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),'-') != IFNULL(position_name,'-') THEN 1
                        ELSE 0
                        END AS lineup_change
                        FROM stack_changes
                        WHERE player_id IS NOT NULL
                        ),
                        iso_changes as (
                        SELECT distinct period, event_timestamp, match_id, team_id, player_id, player, type, position_name
                        FROM id_lineup_change
                        WHERE lineup_change = 1

                        UNION

                        SELECT distinct period, event_timestamp, match_id, team_id, player_id, player, type, position_name
                        FROM get_half
                        
                        ),
                        find_end as (
                        SELECT period, event_timestamp, 
                        LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_timestamp,
                        match_id, team_id, player_id, player, type, 
                        LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type, position_name
                        FROM iso_changes
                        ),
                        initial_intervals as (
                        SELECT distinct period, event_timestamp interval_start, next_event_timestamp interval_end, match_id, team_id, player_id, player, type, next_event_type, position_name
                        FROM find_end
                        WHERE type NOT IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')
                        AND player_id IS NOT NULL
                        ),
                        sub_out as (
                        SELECT distinct player_id, event_timestamp, period, match_id, type
                        FROM stack_changes
                        WHERE type IN ('Substitution - Off', 'Second Yellow', 'Red Card', 'Bad Behaviour') AND player_id IS NOT NULL
                        ),
                        match_intervals_tmp as (
                        SELECT --initial_intervals.*
                        initial_intervals.period, initial_intervals.interval_start, 
                        CASE WHEN sub_out.event_timestamp < IFNULL(initial_intervals.interval_end, CURRENT_DATE)
                        THEN sub_out.event_timestamp
                        ELSE initial_intervals.interval_end
                        END AS interval_end, 
                        initial_intervals.match_id, 
                        initial_intervals.team_id, initial_intervals.player_id, initial_intervals.player, 
                        CASE WHEN sub_out.event_timestamp < IFNULL(initial_intervals.interval_end, CURRENT_DATE)
                        THEN sub_out.type
                        ELSE initial_intervals.type
                        END AS type, initial_intervals.next_event_type, initial_intervals.position_name
                        FROM initial_intervals
                        LEFT JOIN sub_out
                           ON initial_intervals.match_id = sub_out.match_id
                           AND initial_intervals.player_id = sub_out.player_id
                        WHERE (initial_intervals.period < IFNULL(sub_out.period,9999999)
                           OR (initial_intervals.period = IFNULL(sub_out.period,9999999) 
                                 AND initial_intervals.interval_start < sub_out.event_timestamp
                                 )
                           )
                           --AND initial_intervals.match_id = 7581
                           --initial_intervals.match_id = 7298
                        ),
                        non_player_event_lineup_changes as (
                        SELECT period, match_id, team_id, interval_end event_timestamp, 'Team Substition/Dismissal' AS type
                        FROM match_intervals_tmp
                        WHERE UPPER(next_event_type) LIKE '%SUBSTITUTION%' OR next_event_type IN ('Second Yellow', 'Red Card')
                        ),
                        check_other_subs as (

                        SELECT first_check.period, first_check.match_id, first_check.team_id, first_check.event_timestamp, first_check.type, position_name, first_check.player_id, player
                        FROM (SELECT distinct match_intervals_tmp.period, match_intervals_tmp.match_id, match_intervals_tmp.team_id, non_player_event_lineup_changes.event_timestamp, non_player_event_lineup_changes.type, position_name, match_intervals_tmp.player_id, player
                              FROM match_intervals_tmp
                              INNER JOIN non_player_event_lineup_changes
                                 ON match_intervals_tmp.period = non_player_event_lineup_changes.period
                                 AND match_intervals_tmp.match_id = non_player_event_lineup_changes.match_id
                                 AND match_intervals_tmp.team_id = non_player_event_lineup_changes.team_id
                                 AND non_player_event_lineup_changes.event_timestamp >= match_intervals_tmp.interval_start
                                 AND non_player_event_lineup_changes.event_timestamp < match_intervals_tmp.interval_end
                              ) first_check
                        LEFT JOIN (SELECT period, match_id, team_id, player_id, interval_start FROM match_intervals_tmp) check_other
                           ON first_check.period = check_other.period
                           AND first_check.match_id = check_other.match_id
                           AND first_check.team_id = check_other.team_id
                           AND first_check.player_id = check_other.player_id
                           AND first_check.event_timestamp = interval_start
                        WHERE check_other.player_id IS NULL

                        UNION

                        SELECT distinct period, match_id, team_id, interval_start, type, position_name, player_id, player
                        FROM match_intervals_tmp

                        UNION

                        SELECT distinct period, match_id, team_id, interval_end, next_event_type, position_name, player_id, player
                        FROM match_intervals_tmp

                        ),
                        match_intervals as (
                              SELECT *
                              FROM (
                                    SELECT period, match_id, team_id, position_name, player_id, player, event_timestamp interval_start,
                                    LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) interval_end, type,
                                    LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type,

                                    FROM check_other_subs
                                    )
                        WHERE type NOT IN ('Substitution - Off', 'Second Yellow', 'Red Card', 'Half End')                       
                        ),

                         get_country_world_rank as (
                         SELECT 
                         lookup_id.*, conf.Confederation, 
                         CASE 
                         WHEN lookup_id.name LIKE '%Brazzaville%' THEN 134
                         WHEN lookup_id.name LIKE '%Venezuela%' THEN 50
                         WHEN lookup_id.name LIKE '%Korea%' AND lookup_id.name LIKE '%South%' THEN 22
                         ELSE conf.World_Ranking_January_19_2026 
                         END AS World_Ranking_January_19_2026
                         FROM (SELECT id, name
                                    FROM read_parquet('{STATSBOMB_DIR}/reference.parquet') 
                                    WHERE table_name = 'country') lookup_id
                        LEFT JOIN (SELECT *
                                    FROM read_csv('{ADDITIONAL_DIR}/WC_Confederations.csv')) conf
                           ON TRIM(lookup_id.name) = TRIM(conf.country)
                        ),
                        find_country_quartile as (
                        SELECT distinct Confederation, id country_id,
                        World_Ranking_January_19_2026
                        --CASE 
                        --WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.25 THEN 'Q1'
                        --WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.5 THEN 'Q2'
                        --WHEN World_Ranking_January_19_2026 / (SELECT distinct COUNT(distinct IFNULL(World_Ranking_January_19_2026,99999)) FROM get_country_world_rank) <= 0.75 THEN 'Q3'
                        --ELSE 'Q4'
                        --END AS COUNTRY_QUARTILE
                        FROM get_country_world_rank
                        ),
                        get_player_position as (
                        SELECT distinct match_intervals.*, 
                         CASE WHEN POSITION_SIDE IN ('RC','LC') THEN 'C' ELSE POSITION_SIDE END AS POSITION_SIDE_ADJ, 
                         POSITION_TYPE_ALT, POSITION_BEHAVIOR, POSITION_TYPE, get_country.country_id, World_Ranking_January_19_2026 --COUNTRY_QUARTILE, Confederation
                        FROM match_intervals
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/position_type.parquet') pt
                              ON match_intervals.position_name = pt.position_name
                        LEFT JOIN (SELECT distinct player_id, match_id, country_id FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')) get_country
                           ON match_intervals.player_id = get_country.player_id
                           AND match_intervals.match_id = get_country.match_id
                        LEFT JOIN find_country_quartile
                           ON get_country.country_id = find_country_quartile.country_id

                         ),
                        get_ranks as (
                        SELECT team_id, match_id, period, interval_start, interval_end, player_id, --country_id, --POSITION_SIDE_ADJ, POSITION_TYPE_ALT, POSITION_BEHAVIOR, 
                        POSITION_TYPE, World_Ranking_January_19_2026 --COUNTRY_QUARTILE, Confederation
                        --,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ ORDER BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ, player_id) PLAYER_POSITION_SIDE_ADJ_ID_RANK,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE, player_id) PLAYER_POSITION_TYPE_ID_RANK,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT, player_id) PLAYER_POSITION_TYPE_ALT_ID_RANK,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, country_id ORDER BY match_id, team_id, period, interval_start, country_id, player_id) PLAYER_COUNTRY_ID_RANK,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, interval_end ORDER BY match_id, team_id, period, interval_start, interval_end, player_id) PLAYER_SQUAD_RANK
                        
                        FROM get_player_position
                        WHERE interval_start != interval_end
                        ),
                        apply_attr as (
                         
                         SELECT match_id, period, team_id, interval_start, interval_end, --POSITION_TYPE, 
                         CASE WHEN POSITION_TYPE = 'F' THEN 1 ELSE 0 END AS FORWARDS,
                         CASE WHEN POSITION_TYPE = 'M' THEN 1 ELSE 0 END AS MIDFIELDERS,
                         CASE WHEN POSITION_TYPE = 'B' THEN 1 ELSE 0 END AS BACKS,
                         CASE WHEN POSITION_TYPE = 'GK' THEN 1 ELSE 0 END AS GOALKEEPER,
                         World_Ranking_January_19_2026, 
                         
                         CASE WHEN IFNULL(mc.cluster, wc.cluster) = 0 THEN 1 ELSE 0 END AS C0,
                         CASE WHEN IFNULL(mc.cluster, wc.cluster) = 1 THEN 1 ELSE 0 END AS C1,
                         CASE WHEN IFNULL(mc.cluster, wc.cluster) = 2 THEN 1 ELSE 0 END AS C2,
                         CASE WHEN IFNULL(mc.cluster, wc.cluster) = 3 THEN 1 ELSE 0 END AS C3,
                         CASE WHEN IFNULL(mc.cluster, wc.cluster) = 4 THEN 1 ELSE 0 END AS C4,
                         CASE 
                         WHEN mc.cluster IS NOT NULL THEN 'M'
                         WHEN wc.cluster IS NOT NULL THEN 'W'
                         ELSE NULL
                         END AS MEN_WOMEN

                         FROM get_ranks
                         LEFT JOIN (SELECT player_id, cluster
                                       FROM read_parquet('{ANALYSIS_DIR}/Mens_Clustering.parquet')) mc
                              On get_ranks.player_id = mc.player_id
                        LEFT JOIN (SELECT player_id, cluster
                                       FROM read_parquet('{ANALYSIS_DIR}/Womens_Clustering.parquet')) wc
                              On get_ranks.player_id = wc.player_id
                     )

                     SELECT match_id, period, team_id, interval_start, interval_end, MEN_WOMEN,
                     SUM(FORWARDS) FORWARDS, SUM(MIDFIELDERS) MIDFIELDERS, SUM(BACKS) BACKS, SUM(GOALKEEPER) GOALKEEPER,
                     SUM(C0) C0, SUM(C1) C1, SUM(C2) C2, SUM(C3) C3, SUM(C4) C4, 
                     AVG(World_Ranking_January_19_2026) AVERAGE_WR
                     FROM apply_attr
                     GROUP BY match_id, period, team_id, interval_start, interval_end, MEN_WOMEN
                     ORDER BY match_id, team_id, period, interval_start, interval_end, MEN_WOMEN

                     """)#.write_csv('lineup_check.csv')

print(y_coords)


# y_coords = duckdb.sql(f"""

#                          with lineup_check as (
#                         SELECT match_id, team_id, period, interval_start, interval_end, BACKS, MIDFIELDERS, FORWARDS, OVERALL_FORMATION, PLAYERS_ON_PITCH, TEAM_COMPOSITION_PK
#                         FROM read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') 
#                         ),
#                         event_times as (
#                         SELECT id, match_id, team_id, duration, possession, possession_team_id, shot_outcome, period, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp, shot_statsbomb_xg
#                         FROM read_parquet('{STATSBOMB_DIR}/events.parquet') 
#                         WHERE team_id = possession_team_id
#                         ),
#                         get_stats as (
#                         SELECT distinct et.id, et.match_id, et.possession, IFNULL(duration,0) duration, 
#                         lc.team_id EVENT_TEAM_ID,
#                         lc.TEAM_COMPOSITION_PK EVENT_TEAM_COMPOSITION_PK, lc.PLAYERS_ON_PITCH EVENT_TEAM_PLAYERS_ON_PITCH, 
#                         lc.BACKS EVENT_TEAM_BACKS, lc.MIDFIELDERS EVENT_TEAM_MIDFIELDERS, 
#                         lc.FORWARDS EVENT_TEAM_FORWARDS, lc.OVERALL_FORMATION EVENT_TEAM_OVERALL_FORMATION, IFNULL(shot_statsbomb_xg,0) event_team_shot_statsbomb_xg,
#                         CASE WHEN et.team_id = et.possession_team_id AND UPPER(shot_outcome) = 'GOAL' THEN 1 ELSE 0 END AS event_team_goal_scored,

#                         opponent.team_id OPPONENT_TEAM_ID,
#                         opponent.TEAM_COMPOSITION_PK OPPONENT_TEAM_COMPOSITION_PK, opponent.PLAYERS_ON_PITCH OPPONENT_TEAM_PLAYERS_ON_PITCH, 
#                         opponent.BACKS OPPONENT_TEAM_BACKS, opponent.MIDFIELDERS OPPONENT_TEAM_MIDFIELDERS, 
#                         opponent.FORWARDS OPPONENT_TEAM_FORWARDS, opponent.OVERALL_FORMATION OPPONENT_TEAM_OVERALL_FORMATION
#                         FROM event_times et
#                         LEFT JOIN lineup_check lc
#                            ON et.match_id = lc.match_id
#                            AND et.team_id = lc.team_id
#                            AND et.period = lc.period
#                            AND event_timestamp >= lc.interval_start
#                            AND event_timestamp < lc.interval_end
#                         LEFT JOIN lineup_check opponent
#                            ON et.match_id = opponent.match_id
#                            AND et.team_id = opponent.team_id
#                            AND et.period = opponent.period
#                            AND event_timestamp >= opponent.interval_start
#                            AND event_timestamp < opponent.interval_end
#                         ),
#                         agg_possession as (
#                         SELECT possession, EVENT_TEAM_ID, match_id,
#                         EVENT_TEAM_ID, 
#                         EVENT_TEAM_COMPOSITION_PK, 
#                         EVENT_TEAM_PLAYERS_ON_PITCH, 
#                         EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, 
#                         EVENT_TEAM_OVERALL_FORMATION,


#                         SUM(event_team_goal_scored) event_team_goal_scored,
#                         SUM(event_team_shot_statsbomb_xg) event_team_shot_statsbomb_xg,
#                         SUM(duration) duration,

#                         OPPONENT_TEAM_ID, 
#                         OPPONENT_TEAM_COMPOSITION_PK, 
#                         OPPONENT_TEAM_PLAYERS_ON_PITCH, 
#                         OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS, 
#                         OPPONENT_TEAM_OVERALL_FORMATION
#                         FROM get_stats 

#                         GROUP BY possession, EVENT_TEAM_ID, match_id,
#                         EVENT_TEAM_ID, EVENT_TEAM_COMPOSITION_PK, EVENT_TEAM_PLAYERS_ON_PITCH, EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, EVENT_TEAM_OVERALL_FORMATION,
#                         OPPONENT_TEAM_ID, OPPONENT_TEAM_COMPOSITION_PK, OPPONENT_TEAM_PLAYERS_ON_PITCH, OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS, OPPONENT_TEAM_OVERALL_FORMATION
#                         ),
#                         get_poss_type as (
#                         SELECT agg_possession.*, possession_type
#                         FROM agg_possession
#                         LEFT JOIN (SELECT match_id, possession, possession_team_id, possession_type
#                                     FROM read_parquet('{ADDITIONAL_DIR}/possession_types.parquet')) pt
#                             ON agg_possession.match_id = pt.match_id
#                             AND agg_possession.possession = pt.possession
#                         )

#                         SELECT 
#                         --EVENT_TEAM_ID, EVENT_TEAM_COMPOSITION_PK, EVENT_TEAM_PLAYERS_ON_PITCH, 
#                         EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, possession_type,
#                         --EVENT_TEAM_OVERALL_FORMATION,
                        
#                         SUM(event_team_goal_scored) event_team_goal_scored,
#                         SUM(event_team_shot_statsbomb_xg) event_team_shot_statsbomb_xg,
#                         SUM(duration) duration,

#                         --OPPONENT_TEAM_ID, OPPONENT_TEAM_COMPOSITION_PK, OPPONENT_TEAM_PLAYERS_ON_PITCH, 
#                         OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS--, 
#                         --OPPONENT_TEAM_OVERALL_FORMATION


#                         FROM get_poss_type

#                         GROUP BY

#                         EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, possession_type,
#                         OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS

#                         --EVENT_TEAM_ID, EVENT_TEAM_COMPOSITION_PK, EVENT_TEAM_PLAYERS_ON_PITCH, EVENT_TEAM_BACKS, EVENT_TEAM_MIDFIELDERS, EVENT_TEAM_FORWARDS, EVENT_TEAM_OVERALL_FORMATION,
#                         --OPPONENT_TEAM_ID, OPPONENT_TEAM_COMPOSITION_PK, OPPONENT_TEAM_PLAYERS_ON_PITCH, OPPONENT_TEAM_BACKS, OPPONENT_TEAM_MIDFIELDERS, OPPONENT_TEAM_FORWARDS, OPPONENT_TEAM_OVERALL_FORMATION

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)

# y_coords = duckdb.sql(f"""

#                       SELECT match_id, possession, possession_team_id, possession_type
#                       FROM read_parquet('{ADDITIONAL_DIR}/possession_types.parquet')

#                      """)#.write_csv('lineup_check.csv')

# print(y_coords)