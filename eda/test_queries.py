import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

# test_query = duckdb.sql(f"""
#                         SELECT match_id, team_id, period, start_minutes, start_seconds, COUNT(*)
#                         FROM (
#                               SELECT pl.*, player_id, position_type,
#                               CASE WHEN position_type = 'M' THEN 1 ELSE 0 END AS MIDFIELDERS,
#                               CASE WHEN position_type = 'B' THEN 1 ELSE 0 END AS BACKS,
#                               CASE WHEN position_type = 'F' THEN 1 ELSE 0 END AS FORWARDS,
#                               CASE WHEN position_type = 'F' AND position_type_alt = 'CF' THEN 1 ELSE 0 END AS CENTER_FORWARDS,
#                               CASE WHEN position_type = 'M' AND POSITION_BEHAVIOR = 'A' THEN 1 ELSE 0 END AS ATTACKING_MIDFIELDERS,
#                               CASE WHEN position_type = 'M' AND POSITION_BEHAVIOR = 'D' THEN 1 ELSE 0 END AS DEFENDING_MIDFIELDERS
#                               FROM read_parquet('{project_location}/eda/period_lineups.parquet')  pl
#                               LEFT JOIN read_parquet('{project_location}/eda/position_type.parquet') pt
#                                  ON pl.position_name = pt.position_name
#                           )
#                           GROUP BY match_id, team_id, period, start_minutes, start_seconds
#                           HAVING COUNT(*) < 10

#                     """)

# print(test_query)


test_query2 = duckdb.sql(f"""
                         with bad_matches as (
                         SELECT distinct match_id
                         FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                         WHERE IFNULL(to_period,100) < from_period
                         OR from_time = IFNULL(to_time,'N/A')
                         ),
                         initial_dates as (
                        SELECT match_id, team_id, player_id, country_id, position_name, from_time, to_time, from_period, to_period , 
                         strptime('2026-01-01' , '%Y-%m-%d') start_date,
                         CAST(LEFT(from_time, INSTR(from_time, ':') - 1 ) AS INTEGER)  from_minute, 
                         CAST(SUBSTR(from_time, LEN(from_time) - 1) AS INTEGER) from_second,
                         CAST(LEFT(to_time, INSTR(to_time, ':') - 1 ) AS INTEGER)  to_minute, 
                         CAST(SUBSTR(to_time, LEN(to_time) - 1) AS INTEGER) to_second

                         FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                         WHERE match_id NOT IN (SELECT match_id FROM bad_matches)
                         AND from_period IS NOT NULL 
                           ),

                           convert_timestamp as (

                           SELECT initial_dates.*, start_date + TO_MINUTES(from_minute) + TO_SECONDS(from_second) start_date_adj,
                           start_date + TO_MINUTES(to_minute) + TO_SECONDS(to_second) end_date_adj
                           FROM initial_dates
                           WHERE from_period < IFNULL(to_period,100) OR (from_period = to_period AND  end_date_adj > start_date_adj)
                           ),
                           half_timestamps as (
                              SELECT match_id, team_id, period, minute, second, timestamp, start_date + TO_MINUTES(minute) + TO_SECONDS(second) start_date_adj
                              FROM (SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date
                                    FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
                                    WHERE type IN ('Half End', 'Half Start')
                                    )
                                    
                         ),

                        all_lineup_times as (
                           SELECT distinct match_id, team_id, period_tracked, period_time interval_start
                           FROM (SELECT distinct match_id, team_id, start_date_adj period_time, from_period period_tracked
                                 FROM convert_timestamp
                                 WHERE from_period IS NOT NULL AND from_time IS NOT NULL AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

                                 UNION
                                 
                                 SELECT distinct match_id, team_id, end_date_adj, to_period
                                 FROM convert_timestamp
                                 WHERE to_period IS NOT NULL AND to_time IS NOT NULL AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

                                 UNION

                                 SELECT distinct match_id, team_id, start_date_adj, period
                                 FROM half_timestamps
                                 
                                 )
                            ),

                              all_lineup_changes as (
                                 SELECT *
                                 FROM (
                                       SELECT match_id, team_id, period_tracked period, interval_start, LEAD(interval_start,1) OVER (PARTITION BY match_id, team_id, period_tracked ORDER BY match_id, team_id, period_tracked, interval_start) interval_end

                                       FROM all_lineup_times
                                       )
                                 WHERE period IS NOT NULL AND interval_start != IFNULL(interval_end, TODAY()) AND interval_end IS NOT NULL

                                 ),
                           period_start_end as (
                           SELECT match_id, period, 
                           MIN(interval_start) period_start, 
                           MAX(interval_end) period_end
                           FROM all_lineup_changes
                           GROUP BY match_id, period
                           ),

                           player_match_timeline as (

                            SELECT distinct c.match_id, team_id, player_id, country_id, position_name, --from_period, to_period_adjust, 
                            --start_date_adj, end_date_adj, 
                            period player_period, 
                            --period_start, period_end,
                            CASE
                            WHEN period = from_period AND start_date_adj != period_start THEN start_date_adj
                            ELSE period_start
                            END AS player_interval_start,
                            CASE
                            WHEN period = to_period_adjust AND end_date_adj != period_end THEN end_date_adj
                            ELSE period_end           --IFNULL(end_date_adj, period_end)
                            END AS player_interval_end
                            FROM (SELECT convert_timestamp.* , IFNULL(to_period,last_match_period) to_period_adjust
                                    FROM convert_timestamp 
                                    LEFT JOIN (SELECT match_id, MAX(period) last_match_period 
                                                FROM period_start_end 
                                                GROUP BY match_id) last_match_period
                                       ON convert_timestamp.match_id = last_match_period.match_id
                                    --WHERE from_period != IFNULL(to_period,last_match_period)
                                     
                                      ) c
                            LEFT JOIN period_start_end p
                              ON c.match_id = p.match_id
                              AND p.period >= c.from_period
                              AND p.period <= c.to_period_adjust
                           WHERE NOT (p.period = c.from_period  AND start_date_adj > period_start AND end_date_adj > period_end )

                                ),
                                join_player_info as (
                                SELECT match_timestamps.*, player_id, country_id, position_name
                                FROM (SELECT match_id, team_id, period, interval_start, interval_end
                                       FROM all_lineup_changes) match_timestamps
                                 LEFT JOIN player_match_timeline
                                    ON match_timestamps.match_id = player_match_timeline.match_id
                                    AND match_timestamps.team_id = player_match_timeline.team_id
                                    AND match_timestamps.period = player_match_timeline.player_period
                                    AND interval_start >= player_interval_start
                                    AND interval_start < player_interval_end
                              ),
                              all_match_tracking as (
                              SELECT match_id, team_id, period, interval_start, interval_end, player_id, country_id, position_name
                              FROM (SELECT join_player_info.*
                                    FROM join_player_info
                                    WHERE interval_end IS NOT NULL)
                              
                              )

                              

                              SELECT player_match_timeline.*, RANK() OVER (PARTITION BY team_id, player_period, player_interval_start ORDER BY team_id, player_period, player_interval_start, player_id) RANK_RECORD
                              FROM player_match_timeline
                              WHERE match_id = 3794692
                              ORDER BY team_id, player_period, player_interval_start


                              --SELECT *
                              --FROM bad_matches
                              --WHERE match_id = 68334


                              --SELECT *
                              --FROM convert_timestamp
                              --WHERE match_id = 3893828 --AND team_id = 1207 
                              --ORDER BY team_id, player_id


                              --SELECT match_id, team_id, period, interval_start, interval_end, COUNT(*)
                              --FROM all_match_tracking
                              --WHERE match_id NOT IN (SELECT match_id FROM bad_matches)
                              --GROUP BY match_id, team_id, period, interval_start, interval_end
                              --HAVING COUNT(*) < 10

                    """).write_csv('player_intervals.csv')

print(test_query2)




test_query3 = duckdb.sql(f"""

                      SELECT match_id, team_id, player_id, from_period, to_period, from_time, to_time
                      FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet')
                     WHERE match_id = 3794692 AND team_id = 790 AND from_period IS NOT NULL
                     AND from_time != IFNULL(to_time,'N/A')
                     ORDER BY player_id, from_period, from_time
                     --WHERE IFNULL(to_period,100) < from_period

                     """)#.write_csv('player_checks2.csv')

print(test_query3)


test_query4 = duckdb.sql(f"""

                     SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date
                     FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
                     WHERE type IN ('Half End', 'Half Start')
                     AND match_id = 3794692 
                     ORDER BY team_id, period, minute, second


                    """)

print(test_query4)

test_query5 = duckdb.sql(f"""
                         with bad_matches as (
                         SELECT distinct match_id
                         FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                         WHERE IFNULL(to_period,100) < from_period
                         OR from_time = IFNULL(to_time,'N/A')
                         ),
                         initial_dates as (
                        SELECT match_id, team_id, player_id, country_id, position_name, from_time, to_time, from_period, to_period , 
                         strptime('2026-01-01' , '%Y-%m-%d') start_date,
                         CAST(LEFT(from_time, INSTR(from_time, ':') - 1 ) AS INTEGER)  from_minute, 
                         CAST(SUBSTR(from_time, LEN(from_time) - 1) AS INTEGER) from_second,
                         CAST(LEFT(to_time, INSTR(to_time, ':') - 1 ) AS INTEGER)  to_minute, 
                         CAST(SUBSTR(to_time, LEN(to_time) - 1) AS INTEGER) to_second

                         FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                         WHERE match_id NOT IN (SELECT match_id FROM bad_matches)
                         AND from_period IS NOT NULL 
                           ),

                           convert_timestamp as (

                           SELECT initial_dates.*, start_date + TO_MINUTES(from_minute) + TO_SECONDS(from_second) start_date_adj,
                           start_date + TO_MINUTES(to_minute) + TO_SECONDS(to_second) end_date_adj
                           FROM initial_dates
                           WHERE from_period < IFNULL(to_period,100) OR (from_period = to_period AND  end_date_adj > start_date_adj)
                           ),
                           half_timestamps as (
                              SELECT match_id, team_id, period, minute, second, timestamp, start_date + TO_MINUTES(minute) + TO_SECONDS(second) start_date_adj
                              FROM (SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date
                                    FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
                                    WHERE type IN ('Half End', 'Half Start')
                                    )
                                    
                         ),

                        all_lineup_times as (
                           SELECT distinct match_id, team_id, period_tracked, period_time interval_start
                           FROM (SELECT distinct match_id, team_id, start_date_adj period_time, from_period period_tracked
                                 FROM convert_timestamp
                                 WHERE from_period IS NOT NULL AND from_time IS NOT NULL AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

                                 UNION
                                 
                                 SELECT distinct match_id, team_id, end_date_adj, to_period
                                 FROM convert_timestamp
                                 WHERE to_period IS NOT NULL AND to_time IS NOT NULL AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

                                 UNION

                                 SELECT distinct match_id, team_id, start_date_adj, period
                                 FROM half_timestamps
                                 
                                 )
                            )
                     SELECT *
                     FROM all_lineup_times
                     WHERE match_id = 3794692 
                     ORDER BY team_id, period_tracked, interval_start

                    """)

print(test_query5)