import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

duckdb.sql(f"""
                         with half_timestamps as (
                              SELECT distinct match_id, period, minute, second, timestamp
                                    FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                                    WHERE type IN ('Half End', 'Half Start')
                                    
                         ),
                         
                         all_start_end as (
                         SELECT *
                         FROM ( SELECT *
                         FROM (
                           SELECT match_id, period, minute, second, timestamp, 
                              LEAD(minute,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, minute, second) period_end_minute, 
                              LEAD(second,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, minute, second) period_end_second, 
                              LEAD(timestamp,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, minute, second) period_end_timestamp
                            FROM half_timestamps
                            )
                        )
                        
                        WHERE period_end_timestamp IS NOT NULL
                        ) ,

                        all_lineup_times as (
                           SELECT match_id, period_tracked, CAST(LEFT(period_time, INSTR(period_time, ':') - 1 ) AS INTEGER) period_minute, CAST(SUBSTR(period_time, LEN(period_time) - 1) AS INTEGER) period_second
                           FROM (SELECT distinct match_id, from_time period_time, from_period period_tracked
                                 FROM read_parquet('{project_location}/Statsbomb/lineups.parquet') 
                                 WHERE from_period IS NOT NULL 

                                 UNION
                                 
                                 SELECT distinct match_id, to_time, to_period
                                 FROM read_parquet('{project_location}/Statsbomb/lineups.parquet') 
                                 WHERE to_period IS NOT NULL 
                                 )
                            ),
                            match_timestamps as (
                              SELECT distinct match_id, period, period_end_minute, period_end_second
                              FROM (
                              SELECT match_id, period, period_end_minute, period_end_second
                              --SUBSTR(LEFT(period_end_timestamp, INSTR(period_end_timestamp, '.') - 1 ), 4) period_end_str
                              FROM all_start_end

                              UNION

                              SELECT match_id, period_tracked, period_minute, period_second
                              FROM all_lineup_times
                              )
                              ),

                              match_timeline as (
                                 SELECT *
                                 FROM (
                                 SELECT match_id, period, period_end_minute asn_start_minute, period_end_second asn_start_second, 
                                       LEAD(period_end_minute,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, period_end_minute, period_end_second) asn_end_minute, 
                                       LEAD(period_end_second,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, period_end_minute, period_end_second) asn_end_second 
                                 FROM match_timestamps
                                 )
                                 WHERE asn_end_minute IS NOT NULL
                              ),
                              all_lineup_changes as (
                              SELECT check_players.match_id, team_id, player_id, country_id, country_name, from_period, IFNULL(to_period, last_period) to_period, from_time, to_time, position_name
                              FROM read_parquet('{project_location}/Statsbomb/lineups.parquet') check_players
                              LEFT JOIN (
                                 SELECT match_id, MAX(period) last_period
                                 FROM read_parquet('{project_location}/Statsbomb/events.parquet')
                                 GROUP BY match_id) last_period
                              ON check_players.match_id = last_period.match_id
                              WHERE from_period IS NOT NULL 
                           ),
                           match_end as (
                           SELECT mt2.match_id, mt2.period, MAX(IFNULL(asn_end_minute,period_end_minute)) asn_end_minute, MAX(IFNULL(asn_end_second, period_end_second)) asn_end_second
                           FROM all_lineup_changes mt2
                           INNER JOIN all_start_end all_check_again
                              ON mt2.match_id = all_check_again.match_id
                              AND mt2.period = all_check_again.period
                           GROUP BY mt2.match_id, mt2.period
                           ),
                           all_lineup_events as (
                           SELECT all_lineup_changes.match_id, team_id, player_id, country_id, position_name, country_name, from_period, IFNULL(to_period, match_end.period) to_period,
                           CAST(LEFT(from_time, INSTR(from_time, ':') - 1 ) as INTEGER) from_time_minutes, 
                           CAST(SUBSTR(from_time, LEN(from_time) - 1) AS INTEGER) from_time_seconds,
                           IFNULL( CAST(LEFT(to_time, INSTR(to_time, ':') - 1 ) as INTEGER), asn_end_minute) to_time_minutes, 
                           IFNULL( CAST(SUBSTR(to_time, LEN(to_time) - 1) AS INTEGER), asn_end_second) to_time_seconds
                           FROM all_lineup_changes
                           LEFT JOIN match_end
                              ON all_lineup_changes.match_id = match_end.match_id
                           ),
                           adj_player_times as (
                           SELECT al.match_id, player_id, country_id, position_name, period, 
                           CASE
                           WHEN period = from_period THEN from_time_minutes ELSE minute END AS start_minutes,
                           CASE
                           WHEN period = from_period THEN from_time_seconds ELSE second END AS start_seconds, 
                           CASE
                           WHEN period = to_period THEN to_time_minutes ELSE period_end_minute END AS end_minutes,
                           CASE
                           WHEN period = to_period THEN to_time_seconds ELSE period_end_second END AS end_seconds
                           FROM all_lineup_events al
                           LEFT JOIN all_start_end start_end
                              ON al.match_id = start_end.match_id
                              AND start_end.period >= from_period
                              AND start_end.period <= to_period
                           )
                           
                           SELECT *
                           FROM adj_player_times
                    """).write_parquet('period_lineups.parquet')
