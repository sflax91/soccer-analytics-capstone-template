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

y_coords = duckdb.sql(f"""
                     LOAD spatial;
                      

                      with label_zones as (
                        SELECT location_x, location_y,
                        CASE WHEN location_x <= 40 THEN 'L'
                        ELSE 'R'
                        END AS PITCH_ORIENTATION,
                      CASE 
                      WHEN location_x <= 18 AND location_y >= 19.885 AND  location_y <= 60.115 THEN 'Box'
                      WHEN location_x <= 18 AND location_y > 60.115 THEN '1'
                      WHEN location_x > 18 AND location_x <= 60 AND location_y > 60.115 THEN '2'
                      WHEN location_x > 18 AND location_x <= 60 AND location_y <= 60.115 AND location_y >= 19.885 THEN '3'
                     WHEN location_x > 18 AND location_x <= 60 AND location_y < 19.885 THEN '4'
                     WHEN location_x <= 18 AND location_y < 60.885 THEN '5'
                      
                      WHEN location_x >= 102 AND location_y >= 19.885 AND  location_y <= 885 THEN 'Box'
                      WHEN location_x >= 102 AND location_y >= 19.885 AND  location_y <= 60.115 THEN 'Box'
                      WHEN location_x >= 102 AND location_y > 60.115 THEN '6'
                      WHEN location_x < 102 AND location_x > 60 AND location_y > 60.115 THEN '7'
                      WHEN location_x < 102 AND location_x > 60 AND location_y <= 60.115 AND location_y >= 19.885 THEN '8'
                     WHEN location_x < 102 AND location_x > 60 AND location_y < 19.885 THEN '9'
                     WHEN location_x >= 102 AND location_y < 60.885 THEN '10'
                      ELSE NULL 
                      END AS ZONE_LOCATION
                        FROM read_parquet('{project_location}/data/Statsbomb/events.parquet') e
                        WHERE location_y IS NOT NULL AND match_id = 7542
                        )

                        SELECT label_zones.*, 
                        CASE
                        WHEN ZONE_LOCATION = 'Box' THEN 0
                        WHEN ZONE_LOCATION = '1' THEN location_y - 60.115
                        WHEN ZONE_LOCATION = '2' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(12, 60.115)) 
                        WHEN ZONE_LOCATION = '3' THEN location_x - 18
                        WHEN ZONE_LOCATION = '4' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(12, 19.885)) 
                        WHEN ZONE_LOCATION = '5' THEN 19.885 - location_y
                        WHEN ZONE_LOCATION = '6' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(102, 60.115)) 
                        WHEN ZONE_LOCATION = '7' THEN location_y - 60.115
                        WHEN ZONE_LOCATION = '8' THEN 102 - location_x
                        WHEN ZONE_LOCATION = '9' THEN ST_Distance(ST_Point(location_x, location_y), ST_Point(102, 19.885)) 
                        WHEN ZONE_LOCATION = '10' THEN 19.885 - location_y
                        ELSE NULL 
                        END AS PROX_BOX

                        FROM label_zones
 
                    """)
print(y_coords)