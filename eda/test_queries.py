import duckdb
import math
import matplotlib.pyplot as plt
import numpy as np

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
                        # SELECT FULL_SQUAD_GROUPING_ID, OFFENSE_DEFENSE, AVG(shot_statsbomb_xg) avg_shot_statsbomb_xg, COUNT(id) number_of_shots, COUNT(match_id) number_of_matches
                        # FROM get_shot_xg
                        # GROUP BY FULL_SQUAD_GROUPING_ID, OFFENSE_DEFENSE
                        # ORDER BY AVG(shot_statsbomb_xg) DESC
#                     """)

# print(test_query2)

# x_coords = duckdb.sql(f"""
#                         SELECT location_x
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         WHERE location_x IS NOT NULL --AND match_id = 7542
 
#                     """).df()

# y_coords = duckdb.sql(f"""
#                         SELECT location_y
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         WHERE location_y IS NOT NULL --AND match_id = 7542
 
#                     """).df()
#x_coords, y_coords = np.array(xy_coords).T


#print(x_coords)

#plt.scatter(x_coords, y_coords)
#plt.savefig('another_test.png')


# y_coords = duckdb.sql(f"""
                      
#                       SELECT distinct min_x, max_x, min_y, max_y
#                       FROM (
#                         SELECT match_id, MIN(round(location_x)) min_x, MAX(round(location_x)) max_x, MIN(round(location_y)) min_y, MAX(round(location_y)) max_y
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         WHERE location_y IS NOT NULL --AND match_id = 7542
#                         GROUP BY match_id)
 
#                     """)
# print(y_coords)

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
#                         with carries as (
#                         SELECT match_id, id, index_num, period, timestamp, duration, location_x, location_y, possession, possession_team_id, team_id, 
#                          player_id, carry_end_location_x, carry_end_location_y
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet')
#                         WHERE carry_end_location_x IS NOT NULL AND match_id = 15946 
#                         )

#                         SELECT --carries.match_id, carries.period, duration, carries.possession, carries.possession_team_id, carries.team_id, 
#                         player_id, PITCH_ORIENTATION, PITCH_THIRD_ADJ, 
#                         PITCH_THIRD_END_ADJ, distance_traveled, progress_type, THIRDS_ADVANCED, STARTING_DISTANCE_TO_GOAL_SHOOTING_ON, STARTING_DISTANCE_TO_GOAL_DEFENDING, PROGRESS_TO_GOAL_SHOOTING_ON, PROGRESS_TO_GOAL_DEFENDING, EVENT_ZONE_START
#                         FROM carries
#                         LEFT JOIN read_parquet('{project_location}/eda/event_proximity.parquet') ep
#                           ON carries.match_id = ep.match_id
#                           AND carries.id = ep.id
#                           AND carries.period = ep.period

                              


 
#                     """)
# print(y_coords)

y_coords = duckdb.sql(f"""
                        SELECT distinct OVERALL_FORMATION, PLAYERS_ON_PITCH
                        FROM read_parquet('{project_location}/eda/team_composition.parquet') e
                        ORDER BY OVERALL_FORMATION, PLAYERS_ON_PITCH
                     """)
print(y_coords)



y_coords = duckdb.sql(f"""
                           SELECT shot_stat.*, off_tc.OVERALL_FORMATION OFF_FORMATION, off_tc.PLAYERS_ON_PITCH OFF_PLAYERS, def_tc.OVERALL_FORMATION DEF_FORMATION, def_tc.PLAYERS_ON_PITCH DEF_PLAYERS
                           FROM read_parquet('{project_location}/eda/shot_level_stats.parquet') shot_stat
                           LEFT JOIN read_parquet('{project_location}/eda/team_composition.parquet') off_tc
                            ON shot_stat.match_id = off_tc.match_id
                            AND shot_stat.OFF_TEAM_COMPOSITION_PK = off_tc.TEAM_COMPOSITION_PK
                           LEFT JOIN read_parquet('{project_location}/eda/team_composition.parquet') def_tc
                            ON shot_stat.match_id = def_tc.match_id
                            AND shot_stat.DEF_TEAM_COMPOSITION_PK = def_tc.TEAM_COMPOSITION_PK
                           --WHERE shot_stat.match_id = 15956
                           ORDER BY shot_stat.match_id, shot_stat.period, event_date

                     """)
print(y_coords)


# y_coords = duckdb.sql(f"""
#                       with shot_info as (
#                         SELECT e.match_id, --e.id, 
#                          --strptime('2026-01-01' , '%Y-%m-%d') start_date,
#                          --minute, second,
#                         strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_date,
#                          e.period, e.team_id, e.possession, --location_x, location_y, 
#                           --e.possession_team_id, --timestamp, duration, --location_x, location_y, 
#                          player_id, --shot_end_location_x, shot_end_location_y, shot_end_location_z, 
#                         shot_statsbomb_xg, shot_outcome, 
#                         shot_technique, 
#                          shot_body_part, shot_type, 
#                          CASE WHEN shot_first_time THEN 1 ELSE 0 END AS shot_first_time, 
#                          CASE WHEN shot_deflected THEN 1 ELSE 0 END AS shot_deflected, 
#                          CASE WHEN shot_aerial_won THEN 1 ELSE 0 END AS shot_aerial_won, 
#                          CASE WHEN shot_follows_dribble THEN 1 ELSE 0 END AS shot_follows_dribble, 
#                          CASE WHEN shot_one_on_one THEN 1 ELSE 0 END AS shot_one_on_one,
#                          CASE WHEN shot_open_goal THEN 1 ELSE 0 END AS shot_open_goal, 
#                          CASE WHEN shot_redirect THEN 1 ELSE 0 END AS shot_redirect, 
#                          CASE WHEN shot_saved_off_target THEN 1 ELSE 0 END AS shot_saved_off_target, 
#                          CASE WHEN shot_saved_to_post THEN 1 ELSE 0 END AS shot_saved_to_post, 

#                         EVENT_ZONE_START OFF_ZONE, DIST_TO_GOAL
                      
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         LEFT JOIN read_parquet('{project_location}/eda/event_proximity.parquet') ep
#                            ON e.match_id = ep.match_id
#                            AND e.id = ep.id
#                            AND e.period = ep.period
#                           LEFT JOIN read_parquet('{project_location}/data/Statsbomb/matches.parquet') m
#                             ON e.match_id = m.match_id
#                         WHERE shot_end_location_x IS NOT NULL
#                         AND e.match_id = 15956
#                         )

#                         SELECT shot_info.*, off_team.TEAM_GROUPINGS_TIMELINE_PK OFF_TEAM_GROUPINGS_PK, def_team.TEAM_GROUPINGS_TIMELINE_PK DEF_TEAM_GROUPINGS_PK,
#                         home_away.HOME_AWAY OFF_HOME_AWAY, home_away2.team_id DEF_TEAM_ID, home_away2.HOME_AWAY DEF_HOME_AWAY,
#                         IFNULL(IFNULL(score_check1.home_score, score_check2.home_score), score_check3.home_score) home_score,
#                         IFNULL(IFNULL(score_check1.away_score, score_check2.away_score), score_check3.away_score) away_score
#                         FROM shot_info
#                         LEFT JOIN read_parquet('{project_location}/eda/team_groupings_timeline.parquet') off_team
#                           ON shot_info.match_id = off_team.match_id
#                           AND shot_info.team_id = off_team.team_id
#                           AND shot_info.period = off_team.period
#                           AND shot_info.event_date >= off_team.interval_start
#                           AND shot_info.event_date < off_team.interval_end
#                         LEFT JOIN read_parquet('{project_location}/eda/team_groupings_timeline.parquet') def_team
#                           ON shot_info.match_id = def_team.match_id
#                           AND shot_info.team_id != def_team.team_id
#                           AND shot_info.period = def_team.period
#                           AND shot_info.event_date >= def_team.interval_start
#                           AND shot_info.event_date < def_team.interval_end
#                         LEFT JOIN (                        
#                                     SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
#                                     FROM read_parquet('{project_location}/data/Statsbomb/matches.parquet')

#                                     UNION

#                                     SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
#                                     FROM read_parquet('{project_location}/data/Statsbomb/matches.parquet')
#                                     ) home_away
#                            ON shot_info.match_id = home_away.match_id 
#                            AND shot_info.team_id = home_away.team_id 
#                           LEFT JOIN (                        
#                                     SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
#                                     FROM read_parquet('{project_location}/data/Statsbomb/matches.parquet')

#                                     UNION

#                                     SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
#                                     FROM read_parquet('{project_location}/data/Statsbomb/matches.parquet')
#                                     ) home_away2
#                            ON shot_info.match_id = home_away2.match_id 
#                            AND shot_info.team_id != home_away2.team_id 

#                            LEFT JOIN (
#                                     SELECT match_id, start_period, end_period, home_score, away_score, 
#                                     strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(start_minute) + TO_SECONDS(start_second) SCORE_TL_START,
#                                     CASE
#                                     WHEN end_minute IS NULL THEN NULL
#                                     ELSE strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(end_minute) + TO_SECONDS(end_second) 
#                                     END AS SCORE_TL_END
#                                     FROM read_parquet('{project_location}/eda/match_score_timeline.parquet')
#                                     WHERE start_period = end_period AND end_period IS NOT NULL
#                                     ) score_check1
#                             ON shot_info.match_id = score_check1.match_id
#                             AND shot_info.period = score_check1.start_period
#                             AND shot_info.event_date >= score_check1.SCORE_TL_START
#                             AND shot_info.event_date < score_check1.SCORE_TL_END


#                            LEFT JOIN (
#                                     SELECT match_id, start_period, end_period, home_score, away_score, 
#                                     strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(start_minute) + TO_SECONDS(start_second) SCORE_TL_START,
#                                     CASE
#                                     WHEN end_minute IS NULL THEN NULL
#                                     ELSE strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(end_minute) + TO_SECONDS(end_second) 
#                                     END AS SCORE_TL_END
#                                     FROM read_parquet('{project_location}/eda/match_score_timeline.parquet')
#                                     WHERE start_period != end_period AND end_period IS NOT NULL
#                                     ) score_check2
#                             ON shot_info.match_id = score_check2.match_id
#                             AND (
#                                   (shot_info.period = score_check2.start_period AND shot_info.event_date >= score_check2.SCORE_TL_START)
#                                   OR 
#                                   (shot_info.period = score_check2.end_period AND shot_info.event_date < IFNULL(score_check2.SCORE_TL_END, strptime('2027-01-01' , '%Y-%m-%d')))
#                                   OR 
#                                   (shot_info.period > score_check2.start_period AND shot_info.period < score_check2.end_period)
#                                   )

#                              LEFT JOIN (
#                                     SELECT match_id, start_period, end_period, home_score, away_score, 
#                                     strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(start_minute) + TO_SECONDS(start_second) SCORE_TL_START,
#                                     CASE
#                                     WHEN end_minute IS NULL THEN NULL
#                                     ELSE strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(end_minute) + TO_SECONDS(end_second) 
#                                     END AS SCORE_TL_END
#                                     FROM read_parquet('{project_location}/eda/match_score_timeline.parquet')
#                                     WHERE end_period IS NULL
#                                     ) score_check3
#                             ON shot_info.match_id = score_check3.match_id
#                             AND (
#                                   (shot_info.period = score_check3.start_period AND shot_info.event_date >= score_check3.SCORE_TL_START)
#                                   OR 
#                                   (shot_info.period > score_check3.start_period)
#                                   )

                              


 
#                      """)
# print(y_coords)


# y_coords = duckdb.sql(f"""
#                         SELECT e.match_id, --e.id, 
#                          e.period, e.team_id, e.possession, location_x, location_y, 
#                           --e.possession_team_id, --timestamp, duration, --location_x, location_y, 
#                          player_id, --shot_end_location_x, shot_end_location_y, shot_end_location_z, 
#                         shot_statsbomb_xg, --shot_outcome, 
#                         --shot_technique, 
#                          --shot_body_part, shot_type, 
#                          CASE WHEN shot_first_time THEN 1 ELSE 0 END AS shot_first_time, 
#                          CASE WHEN shot_deflected THEN 1 ELSE 0 END AS shot_deflected, 
#                          CASE WHEN shot_aerial_won THEN 1 ELSE 0 END AS shot_aerial_won, 
#                          CASE WHEN shot_follows_dribble THEN 1 ELSE 0 END AS shot_follows_dribble, 
#                          CASE WHEN shot_one_on_one THEN 1 ELSE 0 END AS shot_one_on_one,
#                          CASE WHEN shot_open_goal THEN 1 ELSE 0 END AS shot_open_goal, 
#                          CASE WHEN shot_redirect THEN 1 ELSE 0 END AS shot_redirect, 
#                          CASE WHEN shot_saved_off_target THEN 1 ELSE 0 END AS shot_saved_off_target, 
#                          CASE WHEN shot_saved_to_post THEN 1 ELSE 0 END AS shot_saved_to_post, 
#                         CASE WHEN shot_outcome IN ('Saved', 'Saved to Post', 'Saved Off Target') THEN 1 ELSE 0 END AS shot_saved,
#                       CASE WHEN shot_outcome IN ('Wayward', 'Off T') THEN 1 ELSE 0 END AS shot_off_target,
#                       CASE WHEN shot_outcome = 'Goal' THEN 1 ELSE 0 END AS shot_goal,
#                       CASE WHEN shot_outcome = 'Post' THEN 1 ELSE 0 END AS shot_post,
#                       CASE WHEN shot_outcome = 'Blocked' THEN 1 ELSE 0 END AS shot_blocked,

#                       CASE WHEN shot_technique = 'Lob' THEN 1 ELSE 0 END AS shot_lob,
#                       CASE WHEN shot_technique IN ('Volley','Half Volley') THEN 1 ELSE 0 END AS shot_volley,
#                       CASE WHEN shot_technique = 'Diving Header' THEN 1 ELSE 0 END AS shot_diving_header,
#                       CASE WHEN shot_technique = 'Backheel' THEN 1 ELSE 0 END AS shot_backheel,
#                       CASE WHEN shot_technique = 'Normal' THEN 1 ELSE 0 END AS shot_normal,
#                       CASE WHEN shot_technique = 'Overhead Kick' THEN 1 ELSE 0 END AS shot_overhead_kick,

#                       CASE WHEN shot_body_part = 'Head' THEN 1 ELSE 0 END AS shot_header,
#                       CASE WHEN shot_body_part = 'Left Foot' THEN 1 ELSE 0 END AS shot_left_foot,
#                       CASE WHEN shot_body_part = 'Right Foot ' THEN 1 ELSE 0 END AS shot_right_foot,
#                       CASE WHEN shot_body_part = 'Other' THEN 1 ELSE 0 END AS shot_other,

#                       CASE WHEN shot_type = 'Open Play' THEN 1 ELSE 0 END AS shot_open_play,
#                       CASE WHEN shot_type = 'Free Kick' THEN 1 ELSE 0 END AS shot_free_kick,
#                       CASE WHEN shot_type = 'Kick Off' THEN 1 ELSE 0 END AS shot_kick_off,
#                       CASE WHEN shot_type = 'Corner' THEN 1 ELSE 0 END AS shot_corner,
#                       CASE WHEN shot_type = 'Penalty' THEN 1 ELSE 0 END AS shot_penalty,

#                         EVENT_ZONE_START OFF_ZONE, DIST_TO_GOAL
                      
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         LEFT JOIN read_parquet('{project_location}/eda/event_proximity.parquet') ep
#                            ON e.match_id = ep.match_id
#                            AND e.id = ep.id
#                            AND e.period = ep.period
#                         WHERE shot_end_location_x IS NOT NULL
#                         AND e.match_id = 15946
                              


 
#                      """)
# print(y_coords)

# test2 = duckdb.sql(f"""
#                      LOAD spatial;
                      

#                       with shot_side as (
#                      SELECT distinct match_id, period, team_id, possession_team_id, 
#                       CASE WHEN location_x >= 60 THEN 'R'
#                       ELSE 'L'
#                       END AS SHOOTING_SIDE
#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         WHERE type = 'Shot'
#                         ),
#                         bad_matches_shot_side as (
#                         SELECT match_id
#                         FROM shot_side
#                         GROUP BY match_id, period, possession_team_id
#                         HAVING IFNULL(COUNT(distinct SHOOTING_SIDE),0) != 1
#                         ),
                    
#                     label_zones as (
#                         SELECT match_id, id, index_num, period, minute, second, timestamp, duration, player_id, team_id, possession_team_id, type, location_x, location_y,
#                         CASE WHEN location_x <= 40 THEN 'L'
#                         ELSE 'R'
#                         END AS PITCH_ORIENTATION,
#                       CASE 
#                       WHEN location_x <= 18 AND location_y >= 19.885 AND  location_y <= 60.115 THEN 'Box'
#                       WHEN location_x <= 18 AND location_y > 60.115 THEN '1'
#                       WHEN location_x > 18 AND location_x <= 60 AND location_y > 60.115 THEN '2'
#                       WHEN location_x > 18 AND location_x <= 60 AND location_y <= 60.115 AND location_y >= 19.885 THEN '3'
#                      WHEN location_x > 18 AND location_x <= 60 AND location_y < 19.885 THEN '4'
#                      WHEN location_x <= 18 AND location_y < 60.885 THEN '5'
                      
#                       WHEN location_x >= 102 AND location_y >= 19.885 AND  location_y <= 60.115 THEN 'Box'
#                       WHEN location_x >= 102 AND location_y > 60.115 THEN '6'
#                       WHEN location_x < 102 AND location_x > 60 AND location_y > 60.115 THEN '7'
#                       WHEN location_x < 102 AND location_x > 60 AND location_y <= 60.115 AND location_y >= 19.885 THEN '8'
#                      WHEN location_x < 102 AND location_x > 60 AND location_y < 19.885 THEN '9'
#                      WHEN location_x >= 102 AND location_y < 60.885 THEN '10'
#                       ELSE NULL 
#                       END AS ZONE_LOCATION,


#                       CASE 
#                       WHEN location_x <= 6 AND location_y >= 30.855 AND  location_y <= 49.125 THEN 'Goal Area'
#                       WHEN location_x <= 6 AND location_y > 49.125 THEN '1'
#                       WHEN location_x > 6 AND location_x <= 60 AND location_y > 49.125 THEN '2'
#                       WHEN location_x > 6 AND location_x <= 60 AND location_y <= 49.125 AND location_y >= 30.855 THEN '3'
#                      WHEN location_x > 6 AND location_x <= 60 AND location_y < 30.855 THEN '4'
#                      WHEN location_x <= 6 AND location_y < 60.885 THEN '5'
                      
#                       WHEN location_x >= 114 AND location_y >= 30.855 AND  location_y <= 49.125 THEN 'Goal Area'
#                       WHEN location_x >= 114 AND location_y > 49.125 THEN '6'
#                       WHEN location_x < 114 AND location_x > 60 AND location_y > 49.125 THEN '7'
#                       WHEN location_x < 114 AND location_x > 60 AND location_y <= 49.125 AND location_y >= 30.855 THEN '8'
#                      WHEN location_x < 114 AND location_x > 60 AND location_y < 30.855 THEN '9'
#                      WHEN location_x >= 114 AND location_y < 60.885 THEN '10'
#                       ELSE NULL 
#                       END AS ZONE_LOCATION_2,

#                       CASE
#                       WHEN location_x <= 60 AND location_y > 43.66 THEN '1'
#                       WHEN location_x <= 60 AND location_y <= 43.66 AND location_y >= 36.34 THEN '2'
#                       WHEN location_x <= 60 AND location_y < 36.34 THEN '3'

#                       WHEN location_x > 60 AND location_y > 43.66 THEN '4'
#                       WHEN location_x > 60 AND location_y <= 43.66 AND location_y >= 36.34 THEN '5'
#                       WHEN location_x > 60 AND location_y < 36.34 THEN '6'
#                       ELSE NULL
#                       END AS ZONE_LOCATION_3



#                         FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
#                         WHERE location_y IS NOT NULL AND match_id = 7542
#                         ),
#                         add_prox_box as (

#                         SELECT label_zones.*, 
#                         CASE
#                         WHEN ZONE_LOCATION = 'Box' THEN 0
#                         WHEN ZONE_LOCATION = '1' THEN location_y - 60.115
#                         WHEN ZONE_LOCATION = '2' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(12, 60.115)) 
#                         WHEN ZONE_LOCATION = '3' THEN location_x - 18
#                         WHEN ZONE_LOCATION = '4' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(12, 19.885)) 
#                         WHEN ZONE_LOCATION = '5' THEN 19.885 - location_y
#                         WHEN ZONE_LOCATION = '6' THEN location_y - 60.115
#                         WHEN ZONE_LOCATION = '7' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(102, 60.115))  
#                         WHEN ZONE_LOCATION = '8' THEN 102 - location_x
#                         WHEN ZONE_LOCATION = '9' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(102, 19.885)) 
#                         WHEN ZONE_LOCATION = '10' THEN 19.885 - location_y
#                         ELSE NULL 
#                         END AS PROX_BOX, 
#                         CASE
#                         WHEN ZONE_LOCATION_2 = 'Goal Area' THEN 0
#                         WHEN ZONE_LOCATION_2 = '1' THEN location_y - 49.125
#                         WHEN ZONE_LOCATION_2 = '2' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(6, 49.125)) 
#                         WHEN ZONE_LOCATION_2 = '3' THEN location_x - 6
#                         WHEN ZONE_LOCATION_2 = '4' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(6, 30.855)) 
#                         WHEN ZONE_LOCATION_2 = '5' THEN 30.855 - location_y
#                         WHEN ZONE_LOCATION_2 = '6' THEN location_y - 49.125
#                         WHEN ZONE_LOCATION_2 = '7' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(114, 49.125)) 
#                         WHEN ZONE_LOCATION_2 = '8' THEN 114 - location_x
#                         WHEN ZONE_LOCATION_2 = '9' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(114, 30.855)) 
#                         WHEN ZONE_LOCATION_2 = '10' THEN 30.855 - location_y
#                         ELSE NULL 
#                         END AS GOAL_AREA_DIST, 

#                         CASE
#                         WHEN ZONE_LOCATION_3 = '1' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(0, 43.66)) 
#                         WHEN ZONE_LOCATION_3 = '2' THEN location_x
#                         WHEN ZONE_LOCATION_3 = '3' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(0, 36.34)) 

#                         WHEN ZONE_LOCATION_3 = '4' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(120, 43.66)) 
#                         WHEN ZONE_LOCATION_3 = '5' THEN 120 - location_x
#                         WHEN ZONE_LOCATION_3 = '6' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(120, 36.34)) 

#                         ELSE NULL
#                         END AS DIST_TO_GOAL,

#                         CASE
#                         WHEN location_x <= 40 OR location_x > 80 THEN 'Outer'
#                         ELSE 'Middle'
#                         END AS PITCH_THIRD

#                         FROM label_zones

#                         )

#                         SELECT match_id, id, location_x, location_y, ZONE_LOCATION, ZONE_LOCATION_2, ZONE_LOCATION_3, PROX_BOX, GOAL_AREA_DIST, DIST_TO_GOAL
#                         FROM add_prox_box
#                         WHERE GOAL_AREA_DIST < PROX_BOX

#                                             """)
# print(test2)