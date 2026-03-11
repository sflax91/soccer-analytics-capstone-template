import duckdb
import math
import matplotlib.pyplot as plt
import numpy as np

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'



#print(x_coords)

#plt.scatter(x_coords, y_coords)
#plt.savefig('another_test.png')



#x coords
#0-18 left box
#102-120 right box

#y coords
#40 +- (20.115)
#60.115
#19.885


#left box

#top left
#(60.115, 0)
#top right
#(60.115, 18)
#bottom left
#(19.885, 0)
#bottom right
#(19.885, 18)


#right box

#top left
#(60.115, 102)
#top right
#(60.115, 120)
#bottom left
#(19.885, 102)
#bottom right
#(19.885, 120)

#halfway 60



# y_coords = duckdb.sql(f"""
#                         SELECT GK, BACKS, MIDFIELDERS, FORWARDS, ATTACKING_MIDFIELDERS, DEFENDING_MIDFIELDERS, CENTER_FORWARDS, PLAYERS_ON_PITCH, OVERALL_FORMATION
#                         FROM read_parquet('{project_location}/eda/team_composition.parquet') e
#                         ORDER BY OVERALL_FORMATION, PLAYERS_ON_PITCH
#                      """)
# print(y_coords)

# y_coords = duckdb.sql(f"""

#                         SELECT match_id, period, possession, possession_team_id, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK,
#                         player_advantage, goal_diff, COUNT(*) number_of_duels, SUM(tackles) tackles, SUM(aerial_lost) aerial_lost, SUM(duels_lost) duels_lost, SUM(duels_won) duels_won
#                         FROM read_parquet('{project_location}/eda/duel_level_stats.parquet')
#                         GROUP BY match_id, period, possession, possession_team_id, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK,
#                         player_advantage, goal_diff
                     
#                      """)
# print(y_coords)

# y_coords = duckdb.sql(f"""
                      
#                       with label_shots as (

#                         SELECT match_id, period, possession, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, player_advantage, goal_diff,
#                       CASE WHEN shot_outcome = 'Blocked' THEN 1 ELSE 0 END as shots_blocked,
#                       CASE WHEN shot_outcome IN ('Saved','Saved to Post','Saved Off Target') THEN 1 ELSE 0 END as shots_saved,
#                       CASE WHEN shot_outcome IN ('Post') THEN 1 ELSE 0 END as shots_hit_post,
#                       CASE WHEN shot_outcome IN ('Wayward', 'Off T ') THEN 1 ELSE 0 END as shots_off_target,
#                       CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END as shots_goals
#                         FROM read_parquet('{project_location}/eda/shot_level_stats.parquet')
#                         )

#                         SELECT match_id, period, possession, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, player_advantage, goal_diff, 
#                         SUM(shots_blocked) shots_blocked, SUM(shots_saved) shots_saved, SUM(shots_hit_post) shots_hit_post, SUM(shots_off_target) shots_off_target, SUM(shots_goals) shots_goals
#                         FROM label_shots
#                         GROUP BY match_id, period, possession, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, player_advantage, goal_diff
#                      """)
# print(y_coords)

y_coords = duckdb.sql(f"""
                      SELECT MIDFIELDERS_ATTACKING_GROUP_ID, FORWARDS_ATTACKING_GROUP_ID, team_id,
                      --player_id, 
                      EVENT_ZONE_END ,
                      --MAX(completed_pass_length) max_completed_pass_length, AVG(completed_pass_length) avg_completed_pass_length,
                      SUM(pass_goal_assist) pass_goal_assist, SUM(pass_shot_assist) pass_shot_assist, COUNT(completed_pass_length) completed_passes, COUNT(*) pass_attempts, 
                      --BACKS_DEFENDING_GROUP_ID, MIDFIELDERS_DEFENDING_GROUP_ID, 
                      --FORWARDS_ATTACKING_GROUP_ID, MIDFIELDERS_ATTACKING_GROUP_ID, 
                      MIDFIELDERS_DEFENDING,  BACKS_DEFENDING, MIDFIELDERS_ATTACKING, FORWARDS_ATTACKING,
                      FROM (
                          SELECT 
                          pl.team_id, player_id, EVENT_ZONE_END, --pass_length, 
                          CASE WHEN pass_outcome IS NULL THEN pass_length ELSE NULL END AS completed_pass_length,
                          --CASE WHEN EVENT_ZONE_END = EVENT_ZONE_START THEN 'Within' ELSE 'Outside' END AS PASS_WITHIN_ZONE,
                      dtc.BACKS_GROUPING_ID BACKS_DEFENDING_GROUP_ID, dtc.MIDFIELDERS_GROUPING_ID MIDFIELDERS_DEFENDING_GROUP_ID,
                      otc.FORWARDS_GROUPING_ID FORWARDS_ATTACKING_GROUP_ID, otc.MIDFIELDERS_GROUPING_ID MIDFIELDERS_ATTACKING_GROUP_ID,
                          pass_goal_assist, pass_shot_assist, dtc.MIDFIELDERS MIDFIELDERS_DEFENDING, dtc.BACKS BACKS_DEFENDING, otc.MIDFIELDERS MIDFIELDERS_ATTACKING, otc.FORWARDS FORWARDS_ATTACKING
                          FROM read_parquet('{project_location}/eda/pass_level_stats.parquet') pl
                          LEFT JOIN read_parquet('{project_location}/eda/team_composition.parquet') dtc
                            ON DEF_TEAM_COMPOSITION_PK = dtc.TEAM_COMPOSITION_PK 
                          LEFT JOIN read_parquet('{project_location}/eda/team_composition.parquet') otc
                            ON DEF_TEAM_COMPOSITION_PK = otc.TEAM_COMPOSITION_PK 
                          )
                      WHERE EVENT_ZONE_END LIKE 'O%'
                      AND MIDFIELDERS_ATTACKING_GROUP_ID IS NOT NULL
                      AND MIDFIELDERS_DEFENDING_GROUP_ID IS NOT NULL
                      GROUP BY MIDFIELDERS_ATTACKING_GROUP_ID, FORWARDS_ATTACKING_GROUP_ID,
                      --player_id, 
                      EVENT_ZONE_END, team_id,
                      --BACKS_DEFENDING_GROUP_ID, MIDFIELDERS_DEFENDING_GROUP_ID, 
                      --FORWARDS_ATTACKING_GROUP_ID, MIDFIELDERS_ATTACKING_GROUP_ID, 
                      MIDFIELDERS_DEFENDING,  BACKS_DEFENDING, MIDFIELDERS_ATTACKING, FORWARDS_ATTACKING
                      ORDER BY pass_goal_assist DESC
                     """)
print(y_coords)


# y_coords = duckdb.sql(f"""
                      
#                       SELECT match_id, --period, --possession, 
#                       possession_team_id, --team_id, --OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, 
#                       --player_advantage, goal_diff,
#                       --COUNT(distinct player_id) players_passing, COUNT(distinct player_id) players_receiving_passes, 
#                       AVG(pass_length) avg_pass_length, MAX(pass_length) max_pass_length,
#                       SUM(pass_goal_assist) pass_goal_assist, SUM(pass_shot_assist) pass_shot_assist, SUM(pass_cross) pass_cross, SUM(pass_switch) pass_switch,
#                       SUM(pass_through_ball) pass_through_ball, SUM(pass_aerial_won) pass_aerial_won, SUM(pass_deflected) pass_deflected,  SUM(pass_inswinging) pass_inswinging,
#                       SUM(pass_outswinging) pass_outswinging, SUM(pass_no_touch) pass_no_touch, SUM(pass_cut_back) pass_cut_back, SUM(pass_straight) pass_straight, 
#                       SUM(pass_miscommunication) pass_miscommunication
#                       FROM read_parquet('{project_location}/eda/pass_level_stats.parquet')
#                       WHERE possession_team_id = team_id
#                       GROUP BY match_id, --period, --possession, 
#                       possession_team_id--, --team_id, 
#                       --OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK--, 
#                       --player_advantage, goal_diff
#                      """)
# print(y_coords)

# y_coords = duckdb.sql(f"""
#                         with half_timestamps as (
#                               SELECT match_id, team_id, period, minute, second, timestamp, start_date + TO_MINUTES(minute) + TO_SECONDS(second) period_timestamp, type
#                               FROM (SELECT distinct match_id, team_id, period, minute, second, timestamp, strptime('2026-01-01' , '%Y-%m-%d') start_date, type
#                                     FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') 
#                                     WHERE type IN ('Half End', 'Half Start')
#                                     ),
#                         iso_goals as (
#                         SELECT e.match_id, id, index_num, period, timestamp, minute, second, duration,
#                          CASE WHEN team_id = home_team_id THEN 1 ELSE 0 END AS home_goal,
#                          CASE WHEN team_id = away_team_id THEN 1 ELSE 0 END AS away_goal

#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         LEFT JOIN read_parquet('{project_location}/data/Statsbomb/matches.parquet') m
#                            ON e.match_id = m.match_id
#                         WHERE shot_outcome = 'Goal'
#                         ),
#                         rt_goals as (
#                         SELECT match_id, period, timestamp, minute, second, 
#                         SUM(home_goal) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) home_rt,
#                         SUM(away_goal) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) away_rt
#                         FROM iso_goals
#                         ),
#                         game_start as (
#                         SELECT *
#                         FROM (SELECT distinct match_id FROM iso_goals)
#                         CROSS JOIN (SELECT 1 period, '00:00:00.000' event_timestamp, 0 event_minute, 0 event_second, 0 home_rt, 0 away_rt)
#                         ),
#                         add_game_start as (
#                         SELECT match_id, period, timestamp, minute, second, home_rt, away_rt
#                         FROM rt_goals

#                         UNION

#                         SELECT match_id, period, event_timestamp, event_minute, event_second, home_rt, away_rt
#                         FROM game_start
#                         ),
#                         lead_goal_events as (
#                         SELECT match_id, period start_period, timestamp start_timestamp, minute start_minute, second start_second, home_rt home_score, away_rt away_score,
#                         LEAD(period,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_period,
#                         LEAD(timestamp,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_timestamp,
#                         LEAD(minute,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_minute,
#                         LEAD(second,1) OVER (PARTITION BY match_id ORDER BY match_id, period, minute, second) end_second
#                         FROM add_game_start
#                         ),
#                         create_half_splits as (
#                         SELECT distinct match_id, 2 start_period, '00:00:00.000' event_timestamp, 45 event_minute, 0 event_second, home_score, away_score 
#                         FROM lead_goal_events
#                         WHERE start_period < 2 AND end_period > 1

#                         UNION

#                         SELECT distinct match_id, 3 start_period, '00:00:00.000' event_timestamp, 90 event_minute, 0 event_second, home_score, away_score 
#                         FROM lead_goal_events
#                         WHERE start_period < 3 AND end_period > 2

#                         UNION

#                         SELECT distinct match_id, 4 start_period, '00:00:00.000' event_timestamp, 105 event_minute, 0 event_second, home_score, away_score 
#                         FROM lead_goal_events
#                         WHERE start_period < 4 AND end_period > 3

#                         UNION

#                         SELECT distinct match_id, 5 start_period, '00:00:00.000' event_timestamp, 45 event_minute, 0 event_second, home_score, away_score 
#                         FROM lead_goal_events
#                         WHERE start_period < 5 AND end_period > 4

#                         ),
#                         add_other_splits as (

#                         SELECT match_id, start_period, start_timestamp, start_minute, start_second, home_score, away_score
#                         FROM lead_goal_events

#                         UNION

#                         SELECT match_id, start_period, event_timestamp, event_minute, event_second, home_score, away_score
#                         FROM create_half_splits
#                         )
#                         SELECT match_id, start_period, start_timestamp, start_minute, start_second, home_score, away_score,
#                         LEAD(start_period,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_period,
#                         LEAD(start_timestamp,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_timestamp,
#                         LEAD(start_minute,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_minute,
#                         LEAD(start_second,1) OVER (PARTITION BY match_id ORDER BY match_id, start_period, start_minute, start_second) end_second
#                         FROM add_other_splits              
#                      """)
# print(y_coords)

