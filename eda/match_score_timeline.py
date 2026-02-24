import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'

duckdb.sql(f"""
                        with iso_goals as (
                        SELECT e.match_id, id, index_num, period, timestamp, minute, second, duration,
                         CASE WHEN team_id = home_team_id THEN 1 ELSE 0 END AS home_goal,
                         CASE WHEN team_id = away_team_id THEN 1 ELSE 0 END AS away_goal

                        FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
                        LEFT JOIN read_parquet('{project_location}/data/Statsbomb/matches.parquet') m
                           ON e.match_id = m.match_id
                        WHERE shot_outcome = 'Goal'
                        ),
                        rt_goals as (
                        SELECT match_id, period, timestamp, minute, second, 
                        SUM(home_goal) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) home_rt,
                        SUM(away_goal) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) away_rt
                        FROM iso_goals
                        ),
                        game_start as (
                        SELECT *
                        FROM (SELECT distinct match_id FROM iso_goals)
                        CROSS JOIN (SELECT 1 period, '00:00:00.000' event_timestamp, 0 event_minute, 0 event_second, 0 home_rt, 0 away_rt)
                        ),
                        add_game_start as (
                        SELECT match_id, period, timestamp, minute, second, home_rt, away_rt
                        FROM rt_goals

                        UNION

                        SELECT match_id, period, event_timestamp, event_minute, event_second, home_rt, away_rt
                        FROM game_start
                        ),
                        lead_goal_events as (
                        SELECT match_id, period start_period, timestamp start_timestamp, minute start_minute, second start_second, home_rt home_score, away_rt away_score,
                        LEAD(period,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_period,
                        LEAD(timestamp,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_timestamp,
                        LEAD(minute,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_minute,
                        LEAD(second,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_second
                        FROM add_game_start
                        ),
                        create_half_splits as (
                        SELECT distinct match_id, 2 start_period, '00:00:00.000' event_timestamp, 45 event_minute, 0 event_second, home_score, away_score 
                        FROM lead_goal_events
                        WHERE start_period < 2 AND end_period > 1

                        UNION

                        SELECT distinct match_id, 3 start_period, '00:00:00.000' event_timestamp, 90 event_minute, 0 event_second, home_score, away_score 
                        FROM lead_goal_events
                        WHERE start_period < 3 AND end_period > 2

                        UNION

                        SELECT distinct match_id, 4 start_period, '00:00:00.000' event_timestamp, 105 event_minute, 0 event_second, home_score, away_score 
                        FROM lead_goal_events
                        WHERE start_period < 4 AND end_period > 3

                        UNION

                        SELECT distinct match_id, 5 start_period, '00:00:00.000' event_timestamp, 45 event_minute, 0 event_second, home_score, away_score 
                        FROM lead_goal_events
                        WHERE start_period < 5 AND end_period > 4

                        ),
                        add_other_splits as (

                        SELECT match_id, start_period, start_timestamp, start_minute, start_second, home_score, away_score
                        FROM lead_goal_events

                        UNION

                        SELECT match_id, start_period, event_timestamp, event_minute, event_second, home_score, away_score
                        FROM create_half_splits
                        )
                        SELECT match_id, start_period, start_timestamp, start_minute, start_second, home_score, away_score,
                        LEAD(start_period,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_period,
                        LEAD(start_timestamp,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_timestamp,
                        LEAD(start_minute,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_minute,
                        LEAD(start_second,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_second
                        FROM add_other_splits
                                """).write_parquet('match_score_timeline.parquet')

