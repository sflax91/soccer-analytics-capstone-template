import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

test_query = duckdb.sql(f"""
                        SELECT *
                              FROM read_parquet('{project_location}/eda/period_lineups.parquet')  pl
                              --LEFT JOIN read_parquet('{project_location}/eda/position_type.parquet') pt
                                 --ON pl.position_name = pt.position_name

                    """)

print(test_query)


test_query2 = duckdb.sql(f"""
                  with all_lineups as (
                  SELECT *
                  FROM read_parquet('{project_location}/eda/lineup_players.parquet')
                  ),
                  back_to_rows as (
                  UNPIVOT all_lineups
                  ON position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11
                  INTO 
                        NAME PLAYER_RANK
                        VALUE PLAYER_ID
                  )

                  SELECT *
                  FROM back_to_rows
                              --WHERE match_id = 68334

                    """)#.write_csv('player_tracking2.csv')

print(test_query2)




test_query3 = duckdb.sql(f"""
                  with all_lineups as (
                  SELECT *
                  FROM read_parquet('{project_location}/eda/lineup_players.parquet')
                  ),
                  get_lineup_rank as (
                  SELECT RANK_LINEUP, team_id, period, interval_start, interval_end,
                   GK, BACKS, MIDFIELDERS, ATTACKING_MIDFIELDERS, DEFENDING_MIDFIELDERS, 
                   FORWARDS, CENTER_FORWARDS, ATTACK_FORMATION, MIDFIELD_FORMATION, DEFENSE_FORMATION, OVERALL_FORMATION, PLAYERS_ON_PITCH
                  FROM all_lineups al
                  INNER JOIN read_parquet('{project_location}/eda/lineup_combinations.parquet') lc
                        ON IFNULL(al.position_1, -1) = IFNULL(lc.position_1, -1)
                        AND IFNULL(al.position_2, -1) = IFNULL(lc.position_2, -1)
                        AND IFNULL(al.position_3, -1) = IFNULL(lc.position_3, -1)
                        AND IFNULL(al.position_4, -1) = IFNULL(lc.position_4, -1)
                        AND IFNULL(al.position_5, -1) = IFNULL(lc.position_5, -1) 
                        AND IFNULL(al.position_6, -1) = IFNULL(lc.position_6, -1) 
                        AND IFNULL(al.position_7, -1) = IFNULL(lc.position_7, -1) 
                        AND IFNULL(al.position_8, -1) = IFNULL(lc.position_8, -1) 
                        AND IFNULL(al.position_9, -1) = IFNULL(lc.position_9, -1) 
                        AND IFNULL(al.position_10, -1) = IFNULL(lc.position_10, -1) 
                        AND IFNULL(al.position_11, -1) = IFNULL(lc.position_11, -1)

                  )

                  SELECT lr.*, pl.player_id, country_id, pt.*
                  FROM get_lineup_rank lr
                  INNER JOIN read_parquet('{project_location}/eda/period_lineups.parquet')  pl
                        ON lr.team_id = pl.team_id
                        AND lr.period = pl.period
                        AND lr.interval_start = pl.interval_start
                        AND lr.interval_end = pl.interval_end
                  LEFT JOIN read_parquet('{project_location}/eda/position_type.parquet') pt
                        ON pl.position_name = pt.position_name
                           
                                    
                         

                     """)#.write_csv('player_checks2.csv')

print(test_query3)


test_query4 = duckdb.sql(f"""

                        SELECT *
                        FROM read_parquet('{project_location}/eda/lineup_players.parquet')  pl


                    """)

print(test_query4)



# test_query5 = duckdb.sql(f"""
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
#                               WHERE player_id IS NOT NULL
#                               )
#                               SELECT *
#                               FROM get_player_type

#                     """)

# print(test_query5)