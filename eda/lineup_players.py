import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

duckdb.sql(f"""
                                    SELECT RANK() OVER (ORDER BY position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11) RANK_LINEUP, lineup.*
                                    FROM (SELECT distinct position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11
                                          FROM read_parquet('{project_location}/eda/lineup_combinations.parquet')
                                          ) lineup
 

                    """).write_parquet('lineup_players.parquet')