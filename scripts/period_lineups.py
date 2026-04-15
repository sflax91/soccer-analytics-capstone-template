import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "period_lineups.parquet")

duckdb.sql(f"""
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

                        UNION

                        SELECT *
                        FROM other_events
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
                                                
                        SELECT distinct event_times.period, event_times.event_timestamp, event_times.match_id, all_players.team_id, all_players.player_id, event_times.type, position_name, period_removed
                        FROM (
                              SELECT period, MAX(strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second)) event_timestamp, match_id, type
                              FROM other_events
                              WHERE type IN ('Half Start', 'Half End')
                              GROUP BY  match_id, type, period
                              ) event_times
                        INNER JOIN (SELECT distinct match_id, team_id, player_id, event_timestamp, position_name, period, type
                                       FROM stack_changes
                                       
                                    ) all_players
                              ON event_times.match_id = all_players.match_id
                              AND event_times.period >= all_players.period
                              AND event_times.event_timestamp >= all_players.event_timestamp
                        LEFT JOIN (SELECT match_id, team_id, player_id, period period_removed, event_timestamp timestamp_removed FROM stack_changes WHERE type IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')) check_removal
                          ON event_times.match_id = check_removal.match_id
                          AND all_players.team_id = check_removal.team_id
                          AND all_players.player_id = check_removal.player_id
                        WHERE (NOT (event_times.match_id = all_players.match_id AND event_times.period = all_players.period AND event_times.event_timestamp = all_players.event_timestamp))
                        AND all_players.player_id IS NOT NULL
                        AND (
                              (event_times.type = 'Half Start' AND event_times.period < IFNULL(period_removed,9999)) 
                              OR 
                              (event_times.type = 'Half Start' AND event_times.period = IFNULL(period_removed,9999) AND event_times.event_timestamp < IFNULL(timestamp_removed,CURRENT_TIMESTAMP))
                              OR
                              (event_times.type = 'Half End' AND event_times.period < IFNULL(period_removed,9999))
                              )
                        ),


                        id_lineup_change as (
                        SELECT stack_changes.*,
                        CASE 
                        WHEN IFNULL(LAG(period,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),-1) != IFNULL(period,-1) THEN 1
                        WHEN IFNULL(LAG(position_name,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp),'-') != IFNULL(position_name,'-') THEN 1
                        WHEN IFNULL(LAG(player_id,1) OVER (PARTITION BY match_id, team_id, position_name, period ORDER BY match_id, team_id, position_name, period, event_timestamp),-1) != IFNULL(player_id,-1) THEN 1
                        ELSE 0
                        END AS lineup_change
                        FROM stack_changes
                        WHERE player_id IS NOT NULL
                        ),
                        iso_changes as (
                        SELECT distinct period, event_timestamp, match_id, team_id, player_id, type, position_name
                        FROM id_lineup_change
                        WHERE lineup_change = 1

                        UNION

                        SELECT distinct period, event_timestamp, match_id, team_id, player_id, type, position_name
                        FROM get_half
                        
                        ),
                        find_end as (
                        SELECT period, event_timestamp, 
                        LEAD(event_timestamp,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_timestamp,
                        match_id, team_id, player_id, type, 
                        LEAD(type,1) OVER (PARTITION BY match_id, team_id, player_id, period ORDER BY match_id, team_id, player_id, period, event_timestamp) next_event_type, position_name
                        FROM iso_changes
                        ),
                        initial_intervals as (
                        SELECT distinct period, event_timestamp interval_start, next_event_timestamp interval_end, match_id, team_id, player_id, type, next_event_type, position_name
                        FROM find_end
                        WHERE type NOT IN ('Substitution - Off', 'Half End', 'Second Yellow', 'Red Card', 'Bad Behaviour')
                        AND player_id IS NOT NULL
                        ),
                        apply_other_attr as (
                        SELECT distinct get_all_players.match_id, 
                        get_all_players.player_id,
                        get_all_players.team_id, 
                        get_all_players.period, 
                        get_all_players.interval_start, 
                        get_all_players.interval_end, 
                        POSITION_TYPE, POSITION_TYPE_ALT, POSITION_BEHAVIOR
                        FROM (
                              SELECT team_checkpoints.*, player_id, position_name
                              FROM (

                                    SELECT distinct_times.*, IFNULL(LEAD(interval_start,1) OVER (PARTITION BY distinct_times.match_id, team_id, distinct_times.period ORDER BY distinct_times.match_id, team_id, distinct_times.period, interval_start), max_half_end) interval_end
                                    FROM (
                                          SELECT distinct match_id, team_id, period, interval_start
                                          FROM initial_intervals
                                          ) distinct_times
                                       LEFT JOIN (SELECT match_id, period, MAX(interval_end) max_half_end FROM initial_intervals GROUP BY match_id, period) check_half
                                          ON distinct_times.match_id = check_half.match_id
                                          AND distinct_times.period = check_half.period
                                    ) team_checkpoints
                              LEFT JOIN initial_intervals
                                 ON team_checkpoints.match_id = initial_intervals.match_id
                                 AND team_checkpoints.team_id = initial_intervals.team_id
                                 AND team_checkpoints.period = initial_intervals.period
                                 AND team_checkpoints.interval_start >= initial_intervals.interval_start
                                 AND team_checkpoints.interval_start < initial_intervals.interval_end
                              ) get_all_players 
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/position_type.parquet') pt
                           ON get_all_players.position_name = pt.position_name
                        WHERE interval_start < interval_end 
                        )

                        SELECT *
                        FROM apply_other_attr
                    """).write_parquet(output_path)
print('Period Lineups Done.')