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

# y_coords = duckdb.sql(f"""
                      
#                       SELECT match_id, period, possession, possession_team_id, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, player_advantage, goal_diff,
#                       SUM(duration) total_carry_duration, MAX(duration) max_carry_duration, COUNT(id) carries, COUNT(distinct player_id) players_carries
#                       FROM read_parquet('{project_location}/eda/carry_level_stats.parquet')
#                       GROUP BY match_id, period, possession, possession_team_id, team_id, OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, player_advantage, goal_diff,
#                      """)
# print(y_coords)


y_coords = duckdb.sql(f"""
                      
                      SELECT match_id, --period, --possession, 
                      possession_team_id, team_id, --OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, 
                      player_advantage, goal_diff,
                      COUNT(distinct player_id) players_passing, COUNT(distinct player_id) players_receiving_passes, AVG(pass_length) avg_pass_length, MAX(pass_length) max_pass_length,
                      SUM(pass_goal_assist) pass_goal_assist, SUM(pass_shot_assist) pass_shot_assist, SUM(pass_cross) pass_cross, SUM(pass_switch) pass_switch,
                      SUM(pass_through_ball) pass_through_ball, SUM(pass_aerial_won) pass_aerial_won, SUM(pass_deflected) pass_deflected,  SUM(pass_inswinging) pass_inswinging,
                      SUM(pass_outswinging) pass_outswinging, SUM(pass_no_touch) pass_no_touch, SUM(pass_cut_back) pass_cut_back, SUM(pass_straight) pass_straight, 
                      SUM(pass_miscommunication) pass_miscommunication
                      FROM read_parquet('{project_location}/eda/pass_level_stats.parquet')
                      WHERE possession_team_id = team_id
                      GROUP BY match_id, --period, --possession, 
                      possession_team_id, team_id, --OFF_TEAM_COMPOSITION_PK, DEF_TEAM_COMPOSITION_PK, 
                      player_advantage, goal_diff
                     """)
print(y_coords)