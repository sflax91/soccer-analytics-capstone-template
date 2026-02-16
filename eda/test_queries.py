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
                         OR (IFNULL(to_period,100) = from_period AND from_time >= IFNULL(to_time,'N/A'))
                         ),
                           half_timestamps as (
                              SELECT match_id, team_id, period, minute, second, timestamp, start_date + TO_MINUTES(minute) + TO_SECONDS(second) period_timestamp, type
                              FROM (SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date, type
                                    FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
                                    WHERE type IN ('Half End', 'Half Start')
                                    )
                        ),
                         initial_dates as (
                        SELECT distinct l.match_id, l.team_id, player_id, country_id, position_name, from_time, to_time, from_period, IFNULL(to_period, mp.max_period) to_period , 
                         strptime('2026-01-01' , '%Y-%m-%d') start_date,
                         CAST(LEFT(from_time, INSTR(from_time, ':') - 1 ) AS INTEGER)  from_minute, 
                         CAST(SUBSTR(from_time, LEN(from_time) - 1) AS INTEGER) from_second,
                         IFNULL(CAST(LEFT(to_time, INSTR(to_time, ':') - 1 ) AS INTEGER), mp2.minute)  to_minute, 
                         IFNULL(CAST(SUBSTR(to_time, LEN(to_time) - 1) AS INTEGER), mp2.second) to_second
                         FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') l
                         LEFT JOIN (SELECT match_id, MAX(period) max_period from half_timestamps GROUP BY match_id) mp
                              ON l.match_id = mp.match_id
                        LEFT JOIN (SELECT distinct match_id, team_id, period, minute, second FROM half_timestamps) mp2
                              ON mp.match_id = mp2.match_id
                              AND mp.max_period = mp2.period
                         WHERE l.match_id NOT IN (SELECT match_id FROM bad_matches)
                         AND from_period IS NOT NULL 
                           ),

                           convert_timestamp as (

                           SELECT initial_dates.match_id, initial_dates.team_id, player_id, country_id, position_name, from_time, to_time, from_period, to_period , 
                           start_date, from_minute, from_second, to_minute, to_second,
                           start_date + TO_MINUTES(from_minute) + TO_SECONDS(from_second) start_date_adj,
                           CASE 
                           WHEN to_minute IS NOT NULL THEN start_date + TO_MINUTES(to_minute) + TO_SECONDS(to_second) 
                           ELSE h_end.period_timestamp
                           END AS end_date_adj
                           FROM initial_dates
                        LEFT JOIN (SELECT match_id, MAX(period) max_period from half_timestamps GROUP BY match_id) mp
                              ON initial_dates.match_id = mp.match_id
                           LEFT JOIN (SELECT match_id, period, team_id, MAX(period_timestamp) period_timestamp FROM half_timestamps WHERE type = 'Half End' GROUP BY match_id, period, team_id) h_end
                              ON initial_dates.match_id = h_end.match_id
                              AND initial_dates.team_id = h_end.team_id
                              AND IFNULL(initial_dates.to_period, mp.max_period) = h_end.period
                           WHERE from_period < IFNULL(to_period,100) 
                                    OR (from_period = to_period 
                                          AND to_minute IS NOT NULL 
                                          AND start_date + TO_MINUTES(to_minute) + TO_SECONDS(to_second) > start_date_adj)
                                    OR (from_period = to_period 
                                          AND to_minute IS NULL )

                                    
                         ),

                        all_lineup_times as (
                           SELECT distinct match_id, team_id, period_tracked, period_time interval_start
                           FROM (SELECT distinct match_id, team_id, start_date_adj period_time, from_period period_tracked
                                 FROM convert_timestamp
                                 WHERE from_period IS NOT NULL 
                                          AND from_time IS NOT NULL 
                                          AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

                                 UNION
                                 
                                 SELECT distinct match_id, team_id, end_date_adj, to_period
                                 FROM convert_timestamp
                                 WHERE from_period IS NOT NULL 
                                          AND to_period IS NOT NULL 
                                          AND to_time IS NOT NULL 
                                          AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

                                 UNION

                                 SELECT distinct match_id, team_id, period_timestamp, period
                                 FROM half_timestamps
                                 
                                 )
                            ),

                              all_lineup_changes as (
                                 SELECT *
                                 FROM (
                                       SELECT match_id, team_id, period, interval_start, LEAD(interval_start,1) OVER (PARTITION BY match_id, team_id, period ORDER BY match_id, team_id, period, interval_start) interval_end
                                       FROM (SELECT distinct match_id, team_id, period_tracked period, interval_start FROM all_lineup_times)
                                       )
                                 WHERE period IS NOT NULL 
                                          AND interval_start != IFNULL(interval_end, TODAY()) 
                                          AND interval_end IS NOT NULL

                                 ),
                           period_start_end as (
                           SELECT match_id, period, 
                           MIN(interval_start) period_start, 
                           MAX(interval_end) period_end
                           FROM all_lineup_changes
                           GROUP BY match_id, period
                           ),
                        player_match_timeline as (
                              SELECT distinct player_multi_period.match_id, team_id, player_id, country_id, position_name, period,
                                    start_date_adj player_start_date, end_date_adj player_end_date
                              FROM (SELECT convert_timestamp.*
                                    FROM convert_timestamp
                                    WHERE from_period != to_period) player_multi_period
                              INNER JOIN period_start_end p
                                    ON player_multi_period.match_id = p.match_id
                                    AND period = from_period
                              WHERE start_date_adj < end_date_adj
                              

                              UNION

                              SELECT distinct player_multi_period2.match_id, team_id, player_id, country_id, position_name, period,
                                    period_start , period_end
                              FROM (SELECT convert_timestamp.*
                                    FROM convert_timestamp
                                    WHERE from_period != to_period) player_multi_period2
                              INNER JOIN period_start_end p3
                                    ON player_multi_period2.match_id = p3.match_id
                                    AND period > from_period
                                    AND period < to_period

                              UNION

                              SELECT distinct player_multi_period3.match_id, team_id, player_id, country_id, position_name, period,
                                    start_date_adj, end_date_adj
                              FROM (SELECT convert_timestamp.*
                                    FROM convert_timestamp
                                    WHERE from_period != to_period) player_multi_period3
                              INNER JOIN period_start_end p4
                                    ON player_multi_period3.match_id = p4.match_id
                                    AND period = to_period
                              WHERE start_date_adj < end_date_adj

                              UNION

                              SELECT distinct player_multi_period4.match_id, team_id, player_id, country_id, position_name, period,
                                    start_date_adj , end_date_adj
                              FROM (SELECT convert_timestamp.*
                                    FROM convert_timestamp
                                    WHERE from_period = to_period) player_multi_period4
                              INNER JOIN period_start_end p5
                                    ON player_multi_period4.match_id = p5.match_id
                                    AND period = from_period
                              WHERE start_date_adj < end_date_adj

                        ),
                        match_intervals as (
                        SELECT distinct alc.*, player_id, country_id, position_name
                        FROM all_lineup_changes alc
                        LEFT JOIN player_match_timeline pmt
                              ON alc.match_id = pmt.match_id
                              AND alc.team_id = pmt.team_id
                              AND alc.period = pmt.period
                              AND alc.interval_start >= player_start_date
                              AND alc.interval_start < player_end_date
                        )

                        SELECT match_id, team_id, player_id, from_period, to_period, from_time, to_time
                        FROM (
                        SELECT lineup.*, LEAD(from_time,1) OVER (PARTITION BY match_id, team_id, player_id ORDER BY match_id, team_id, player_id, from_period, to_period, from_time) next_time
                        FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') lineup
                        )
                        WHERE next_time != to_time
                        AND next_time IS NOT NULL
                        ORDER BY team_id, player_id, from_period, to_period, from_time


                        --SELECT *
                        --FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                        --WHERE match_id = 3749117 and team_id = 1
                        --ORDER BY team_id, player_id, from_period

                              --SELECT player_match_timeline.*
                              --FROM player_match_timeline
                              --WHERE match_id = 3794692 AND team_id = 790
                              --ORDER BY team_id, period, player_id, player_start_date--, interval_start--, player_start_date--, player_id, start_date_adj--, player_start_date


                              --SELECT *
                              --FROM bad_matches
                              --WHERE match_id = 68334

                    """)#.write_csv('player_tracking2.csv')

print(test_query2)




# test_query3 = duckdb.sql(f"""

#                               with get_player_type as (
#                               SELECT mi.*, player_id, 
#                               CASE WHEN position_type = 'GK' THEN 1 ELSE 0 END AS GK,
#                               CASE WHEN position_type = 'M' THEN 1 ELSE 0 END AS MIDFIELDERS,
#                               CASE WHEN position_type = 'B' THEN 1 ELSE 0 END AS BACKS,
#                               CASE WHEN position_type = 'F' THEN 1 ELSE 0 END AS FORWARDS,
#                               CASE WHEN position_type = 'F' AND position_type_alt = 'CF' THEN 1 ELSE 0 END AS CENTER_FORWARDS,
#                               CASE WHEN position_type = 'M' AND POSITION_BEHAVIOR = 'A' THEN 1 ELSE 0 END AS ATTACKING_MIDFIELDERS,
#                               CASE WHEN position_type = 'M' AND POSITION_BEHAVIOR = 'D' THEN 1 ELSE 0 END AS DEFENDING_MIDFIELDERS
#                               FROM read_parquet('{project_location}/eda/period_lineups.parquet')  mi
#                               LEFT JOIN read_parquet('{project_location}/eda/position_type.parquet') pt
#                                  ON mi.position_name = pt.position_name
#                               )
                              
#                               SELECT *
#                               FROM get_player_type
#                               WHERE player_id IS NULL
                           
                                    
                         

#                      """)#.write_csv('player_checks2.csv')

# print(test_query3)


# test_query4 = duckdb.sql(f"""

#                      SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date
#                      FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
#                      WHERE type IN ('Half End', 'Half Start')
#                      AND match_id = 3794692 
#                      ORDER BY team_id, period, minute, second


#                     """)

# print(test_query4)

# test_query5 = duckdb.sql(f"""
#                          with bad_matches as (
#                          SELECT distinct match_id
#                          FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
#                          WHERE IFNULL(to_period,100) < from_period
#                          OR from_time = IFNULL(to_time,'N/A')
#                          ),
#                          initial_dates as (
#                         SELECT match_id, team_id, player_id, country_id, position_name, from_time, to_time, from_period, to_period , 
#                          strptime('2026-01-01' , '%Y-%m-%d') start_date,
#                          CAST(LEFT(from_time, INSTR(from_time, ':') - 1 ) AS INTEGER)  from_minute, 
#                          CAST(SUBSTR(from_time, LEN(from_time) - 1) AS INTEGER) from_second,
#                          CAST(LEFT(to_time, INSTR(to_time, ':') - 1 ) AS INTEGER)  to_minute, 
#                          CAST(SUBSTR(to_time, LEN(to_time) - 1) AS INTEGER) to_second

#                          FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
#                          WHERE match_id NOT IN (SELECT match_id FROM bad_matches)
#                          AND from_period IS NOT NULL 
#                            ),

#                            convert_timestamp as (

#                            SELECT initial_dates.*, start_date + TO_MINUTES(from_minute) + TO_SECONDS(from_second) start_date_adj,
#                            start_date + TO_MINUTES(to_minute) + TO_SECONDS(to_second) end_date_adj
#                            FROM initial_dates
#                            WHERE from_period < IFNULL(to_period,100) OR (from_period = to_period AND  end_date_adj > start_date_adj)
#                            ),
#                            half_timestamps as (
#                               SELECT match_id, team_id, period, minute, second, timestamp, start_date + TO_MINUTES(minute) + TO_SECONDS(second) start_date_adj
#                               FROM (SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date
#                                     FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
#                                     WHERE type IN ('Half End', 'Half Start')
#                                     )
                                    
#                          ),

#                         all_lineup_times as (
#                            SELECT distinct match_id, team_id, period_tracked, period_time interval_start
#                            FROM (SELECT distinct match_id, team_id, start_date_adj period_time, from_period period_tracked
#                                  FROM convert_timestamp
#                                  WHERE from_period IS NOT NULL AND from_time IS NOT NULL AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

#                                  UNION
                                 
#                                  SELECT distinct match_id, team_id, end_date_adj, to_period
#                                  FROM convert_timestamp
#                                  WHERE to_period IS NOT NULL AND to_time IS NOT NULL AND IFNULL(start_date_adj,TODAY()) != IFNULL(end_date_adj,TODAY())

#                                  UNION

#                                  SELECT distinct match_id, team_id, start_date_adj, period
#                                  FROM half_timestamps
                                 
#                                  )
#                             )
#                      SELECT *
#                      FROM all_lineup_times
#                      WHERE match_id = 3794692 
#                      ORDER BY team_id, period_tracked, interval_start

#                     """)

# print(test_query5)