import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'

possession_tl = duckdb.sql(f"""
                           LOAD spatial;

                           with match_events as (
                                SELECT id, index_num, period, minute, second, timestamp,duration, location_x, location_y, possession, possession_team_id, type, 
                                    match_id, player_id, position_id, play_pattern
                                FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                                WHERE match_id = 15973 AND type NOT IN ('Starting XI','Half Start', 'Half End','Ball Receipt*', 'Ball Recovery')
                                AND location_x IS NOT NULL
                                AND location_y IS NOT NULL
                                ),
                                get_next as (
                                SELECT match_events.*, LEAD(location_x,1) OVER (PARTITION BY match_id ORDER BY match_id, index_num) next_location_x, LEAD(location_y,1) OVER (PARTITION BY match_id ORDER BY match_id, index_num) next_location_y
                                FROM match_events
                                ),
                                calc_event_dist as (
                                SELECT get_next.*, ST_Distance(ST_Point(location_x, location_y), ST_Point(next_location_x, next_location_y)) euclidean_progess
                                FROM get_next
                                )
                                SELECT CAST(possession as VARCHAR), SUM(euclidean_progess)
                                FROM calc_event_dist
                                GROUP BY CAST(possession as VARCHAR)
                                ORDER BY SUM(euclidean_progess) DESC
                                """)
#print(possession_tl)

possession_tl = duckdb.sql(f"""
                           LOAD spatial;

                           with match_events as (
                                SELECT id, index_num, period, minute, second, timestamp,duration, location_x, location_y, possession, possession_team_id, type, 
                                    match_id, player_id, position_id, play_pattern, CAST(possession as VARCHAR) AS possession_char
                                FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                                WHERE match_id = 15973 AND type NOT IN ('Starting XI','Half Start', 'Half End','Ball Receipt*', 'Ball Recovery')
                                AND location_x IS NOT NULL
                                AND location_y IS NOT NULL
                                ),
                                change_poss as (
                                SELECT match_events.*, 
                                CASE WHEN IFNULL(LAG(possession_char,1) OVER (PARTITION BY match_id ORDER BY match_id, index_num),'-1') != possession_char THEN 1 
                                ELSE 0
                                END AS poss_change
                                FROM match_events
                                ),
                                poss_starts as (
                                SELECT *
                                FROM change_poss
                                WHERE poss_change = 1
                                ),
                                next_poss_stats as (
                                SELECT poss_starts.*, LEAD(location_x,1) OVER (PARTITION BY match_id ORDER BY match_id, index_num) next_location_x, LEAD(location_y,1) OVER (PARTITION BY match_id ORDER BY match_id, index_num) next_location_y
                                FROM poss_starts
                                ),
                                calc_poss_progress as (
                                SELECT next_poss_stats.*, ST_Distance(ST_Point(location_x, location_y), ST_Point(next_location_x, next_location_y)) euclidean_progess
                                FROM next_poss_stats
                                )
                                SELECT *
                                FROM calc_poss_progress
                                ORDER BY euclidean_progess DESC
                                """)
print(possession_tl)