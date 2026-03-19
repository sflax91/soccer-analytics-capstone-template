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
                        get_player_position as (
                        SELECT match_intervals.*, 
                         CASE WHEN POSITION_SIDE IN ('RC','LC') THEN 'C' ELSE POSITION_SIDE END AS POSITION_SIDE_ADJ, 
                         POSITION_TYPE_ALT, POSITION_BEHAVIOR, POSITION_TYPE, country_id
                        FROM match_intervals
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/position_type.parquet') pt
                              ON match_intervals.position_name = pt.position_name
                        LEFT JOIN (SELECT distinct player_id, match_id, country_id FROM read_parquet('{STATSBOMB_DIR}/lineups.parquet')) get_country
                           ON match_intervals.player_id = get_country.player_id
                           AND match_intervals.match_id = get_country.match_id

                         ),
                        get_ranks as (
                        SELECT team_id, match_id, period, interval_start, interval_end, player_id, country_id, POSITION_SIDE_ADJ, POSITION_TYPE_ALT, POSITION_BEHAVIOR, POSITION_TYPE,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ ORDER BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ, player_id) PLAYER_POSITION_SIDE_ADJ_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE, player_id) PLAYER_POSITION_TYPE_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT, player_id) PLAYER_POSITION_TYPE_ALT_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, country_id ORDER BY match_id, team_id, period, interval_start, country_id, player_id) PLAYER_COUNTRY_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, interval_end ORDER BY match_id, team_id, period, interval_start, interval_end, player_id) PLAYER_SQUAD_RANK
                        
                        FROM get_player_position
                        WHERE interval_start != interval_end
                        )
                        SELECT gr.team_id,  gr.match_id, gr.period, gr.interval_start, gr.interval_end, gr.player_id, gr.country_id, gr.POSITION_SIDE_ADJ, gr.POSITION_TYPE, gr.POSITION_TYPE_ALT, gr.PLAYER_POSITION_SIDE_ADJ_ID_RANK, gr.POSITION_BEHAVIOR, gr.PLAYER_POSITION_TYPE_ALT_ID_RANK, gr.PLAYER_COUNTRY_ID_RANK, gr.PLAYER_SQUAD_RANK, gr.PLAYER_POSITION_TYPE_ID_RANK,
                        SUM(CASE WHEN gr.country_id = gr2.country_id THEN 1 ELSE 0 END) PLAYERS_SAME_COUNTRY,
                        SUM(CASE WHEN gr.country_id != gr2.country_id THEN 1 ELSE 0 END) PLAYERS_DIFF_COUNTRY,
                        SUM(CASE WHEN gr.POSITION_TYPE = gr2.POSITION_TYPE AND gr.country_id = gr2.country_id THEN 1 ELSE 0 END) POSITION_SAME_COUNTRY,
                        SUM(CASE WHEN gr.POSITION_TYPE = gr2.POSITION_TYPE AND gr.country_id != gr2.country_id THEN 1 ELSE 0 END) POSITION_DIFF_COUNTRY,
                        SUM(CASE WHEN gr.POSITION_SIDE_ADJ = gr2.POSITION_SIDE_ADJ AND gr.country_id = gr2.country_id THEN 1 ELSE 0 END) SIDE_SAME_COUNTRY,
                        SUM(CASE WHEN gr.POSITION_SIDE_ADJ = gr2.POSITION_SIDE_ADJ AND gr.country_id != gr2.country_id THEN 1 ELSE 0 END) SIDE_DIFF_COUNTRY

                        FROM get_ranks gr
                        LEFT JOIN get_ranks gr2
                              ON gr.team_id = gr2.team_id
                              AND gr.match_id = gr2.match_id
                              AND gr.period = gr2.period
                              AND gr.interval_start = gr2.interval_start
                              AND gr.interval_end = gr2.interval_end
                              AND gr.player_id != gr2.player_id
                        GROUP BY gr.team_id,  gr.match_id, gr.period, gr.interval_start, gr.interval_end, gr.player_id, gr.country_id, gr.POSITION_SIDE_ADJ, gr.POSITION_TYPE, gr.POSITION_TYPE_ALT, gr.PLAYER_POSITION_SIDE_ADJ_ID_RANK, gr.POSITION_BEHAVIOR, gr.PLAYER_POSITION_TYPE_ALT_ID_RANK, gr.PLAYER_COUNTRY_ID_RANK, gr.PLAYER_SQUAD_RANK, gr.PLAYER_POSITION_TYPE_ID_RANK
                    """).write_parquet(output_path)
