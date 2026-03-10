import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'

duckdb.sql(f"""
                       with score_info as (
                       SELECT match_id, period, start_date, end_date, home_goals, away_goals
                       FROM read_parquet('{project_location}/eda/match_score_timeline.parquet')
                       ),
                       track_times as (
                       SELECT match_id, period, interval_start
                       FROM read_parquet('{project_location}/eda/period_lineups.parquet')

                       UNION

                       SELECT match_id, period, start_date
                       FROM score_info
                       ),
                       id_changes as (
                       SELECT track_times.*, home_goals, away_goals, player_id, country_id, POSITION_SIDE_ADJ, POSITION_TYPE, POSITION_TYPE_ALT, POSITION_BEHAVIOR, PLAYERS_SAME_COUNTRY, 
                       PLAYERS_DIFF_COUNTRY, POSITION_SAME_COUNTRY, POSITION_DIFF_COUNTRY, SIDE_SAME_COUNTRY, SIDE_DIFF_COUNTRY,

                       CASE
                       WHEN IFNULL(LAG(track_times.period,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != track_times.period THEN 1
                       WHEN IFNULL(LAG(home_goals,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != home_goals THEN 1
                       WHEN IFNULL(LAG(away_goals,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != away_goals THEN 1
                       WHEN IFNULL(LAG(POSITION_SIDE_ADJ,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),'N/A') != POSITION_SIDE_ADJ THEN 1
                       WHEN IFNULL(LAG(POSITION_TYPE,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),'N/A') != POSITION_TYPE THEN 1
                       WHEN IFNULL(LAG(POSITION_TYPE_ALT,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),'N/A') != POSITION_TYPE_ALT THEN 1
                       WHEN IFNULL(LAG(POSITION_BEHAVIOR,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),'N/A') != POSITION_BEHAVIOR THEN 1
                       WHEN IFNULL(LAG(PLAYERS_SAME_COUNTRY,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != PLAYERS_SAME_COUNTRY THEN 1
                       WHEN IFNULL(LAG(PLAYERS_DIFF_COUNTRY,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != PLAYERS_DIFF_COUNTRY THEN 1
                       WHEN IFNULL(LAG(POSITION_SAME_COUNTRY,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != POSITION_SAME_COUNTRY THEN 1
                       WHEN IFNULL(LAG(POSITION_DIFF_COUNTRY,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != POSITION_DIFF_COUNTRY THEN 1
                       WHEN IFNULL(LAG(SIDE_SAME_COUNTRY,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != SIDE_SAME_COUNTRY THEN 1
                       WHEN IFNULL(LAG(SIDE_DIFF_COUNTRY,1) OVER (PARTITION BY track_times.match_id, track_times.period, player_id ORDER BY track_times.match_id, track_times.period, player_id, track_times.interval_start),-1) != SIDE_DIFF_COUNTRY THEN 1
                       ELSE 0
                       END AS iso_row
                       FROM track_times
                       LEFT JOIN score_info
                        ON track_times.match_id = score_info.match_id
                        AND track_times.period = score_info.period
                        AND track_times.interval_start >= score_info.start_date
                        AND track_times.interval_start < IFNULL(score_info.end_date, TODAY())
                        LEFT JOIN read_parquet('{project_location}/eda/period_lineups.parquet') pl
                        ON track_times.match_id = pl.match_id
                        AND track_times.period = pl.period
                        AND track_times.interval_start >= pl.interval_start
                        AND track_times.interval_start < IFNULL(pl.interval_end, TODAY())
                        )

                        SELECT iso_changes.*, LEAD(interval_start) OVER (PARTITION BY match_id, period, player_id ORDER BY match_id, period, player_id, interval_start) interval_end
                        FROM (
                        SELECT match_id, period, home_goals, away_goals, player_id, country_id, POSITION_SIDE_ADJ, POSITION_TYPE, POSITION_TYPE_ALT, POSITION_BEHAVIOR, PLAYERS_SAME_COUNTRY, 
                       PLAYERS_DIFF_COUNTRY, POSITION_SAME_COUNTRY, POSITION_DIFF_COUNTRY, SIDE_SAME_COUNTRY, SIDE_DIFF_COUNTRY, interval_start
                       FROM id_changes
                       WHERE iso_row = 1
                       ) iso_changes

                       ORDER BY match_id, period, player_id, interval_start

                                """).write_parquet('player_match_timeline_with_score.parquet')

