import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

# test_query = duckdb.sql(f"""
#                         SELECT *
#                         FROM read_parquet('{project_location}/data/Statsbomb/matches.parquet') 

#                         """
#                         )#.write_csv('match_investigate.csv')
# print(test_query.columns)

# test_query2 = duckdb.sql(f"""
#                          with get_all_id as (
#                         SELECT distinct match_id, team_id, period, interval_start, GROUP_ATTRIBUTE, GROUP_NAME, UNIT_GROUPING_RANK_ID
#                          FROM read_parquet('{project_location}/eda/stack_lineup_groups.parquet')  st
#                          LEFT JOIN read_parquet('{project_location}/eda/unique_player_combos.parquet') pc
#                               ON IFNULL(st.position_1,-1) = IFNULL(pc.position_1,-1) 
#                               AND IFNULL(st.position_2,-1) = IFNULL(pc.position_2,-1) 
#                               AND IFNULL(st.position_3,-1) = IFNULL(pc.position_3,-1) 
#                               AND IFNULL(st.position_4,-1) = IFNULL(pc.position_4,-1) 
#                               AND IFNULL(st.position_5,-1) = IFNULL(pc.position_5,-1) 
#                               AND IFNULL(st.position_6,-1) = IFNULL(pc.position_6,-1) 
#                               AND IFNULL(st.position_7,-1) = IFNULL(pc.position_7,-1) 
#                               AND IFNULL(st.position_8,-1) = IFNULL(pc.position_8,-1) 
#                               AND IFNULL(st.position_9,-1) = IFNULL(pc.position_9,-1) 
#                               AND IFNULL(st.position_10,-1) = IFNULL(pc.position_10,-1) 
#                               AND IFNULL(st.position_11,-1) = IFNULL(pc.position_11,-1) 
#                         ),
#                         center_of_pitch as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID CENTER_POSITION_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Side' AND GROUP_ATTRIBUTE = 'C'
#                         ),
#                         left_of_pitch as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID LEFT_POSITION_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Side' AND GROUP_ATTRIBUTE = 'L'
#                         ),
#                         right_of_pitch as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID RIGHT_POSITION_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Side' AND GROUP_ATTRIBUTE = 'R'
#                         ),
#                         forwards as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID FORWARDS_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'F'
#                         ),
#                         midfielders as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID MIDFIELDERS_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'M'
#                         ),
#                         backs as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID BACKS_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'B'
#                         ),
#                         gk as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID GK_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'GK'
#                         ),
#                         full_squad as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID FULL_SQUAD_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Full Squad'                     
#                         )

#                         SELECT get_id.*, CENTER_POSITION_GROUPING_ID, LEFT_POSITION_GROUPING_ID, RIGHT_POSITION_GROUPING_ID, FORWARDS_GROUPING_ID, MIDFIELDERS_GROUPING_ID, BACKS_GROUPING_ID, GK_GROUPING_ID, FULL_SQUAD_GROUPING_ID
#                         FROM (SELECT distinct match_id, team_id, period, interval_start FROM get_all_id ) get_id
#                         LEFT JOIN center_of_pitch
#                               ON get_id.match_id = center_of_pitch.match_id
#                               AND get_id.team_id = center_of_pitch.team_id
#                               AND get_id.period = center_of_pitch.period
#                               AND get_id.interval_start = center_of_pitch.interval_start
#                         LEFT JOIN left_of_pitch
#                               ON get_id.match_id = left_of_pitch.match_id
#                               AND get_id.team_id = left_of_pitch.team_id
#                               AND get_id.period = left_of_pitch.period
#                               AND get_id.interval_start = left_of_pitch.interval_start
#                         LEFT JOIN right_of_pitch
#                               ON get_id.match_id = right_of_pitch.match_id
#                               AND get_id.team_id = right_of_pitch.team_id
#                               AND get_id.period = right_of_pitch.period
#                               AND get_id.interval_start = right_of_pitch.interval_start
#                         LEFT JOIN forwards
#                               ON get_id.match_id = forwards.match_id
#                               AND get_id.team_id = forwards.team_id
#                               AND get_id.period = forwards.period
#                               AND get_id.interval_start = forwards.interval_start
#                         LEFT JOIN midfielders
#                               ON get_id.match_id = midfielders.match_id
#                               AND get_id.team_id = midfielders.team_id
#                               AND get_id.period = midfielders.period
#                               AND get_id.interval_start = midfielders.interval_start
#                         LEFT JOIN backs
#                               ON get_id.match_id = backs.match_id
#                               AND get_id.team_id = backs.team_id
#                               AND get_id.period = backs.period
#                               AND get_id.interval_start = backs.interval_start
#                         LEFT JOIN gk
#                               ON get_id.match_id = gk.match_id
#                               AND get_id.team_id = gk.team_id
#                               AND get_id.period = gk.period
#                               AND get_id.interval_start = gk.interval_start
#                         LEFT JOIN full_squad
#                               ON get_id.match_id = full_squad.match_id
#                               AND get_id.team_id = full_squad.team_id
#                               AND get_id.period = full_squad.period
#                               AND get_id.interval_start = full_squad.interval_start
#                     """)

# print(test_query2)

test_query3 = duckdb.sql(f"""
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




                    """)#.write_csv('player_composition.csv')

print(test_query3)