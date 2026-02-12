import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

lineups = duckdb.sql(f"""
                           SELECT match_id, team_id, player_id, country_id, country_name, position_name, from_time, to_time, from_period, to_period
                            FROM read_parquet('{project_location}/Statsbomb/lineups.parquet') 
                            WHERE match_id = 15946
                    """)#.write_csv('lineups_eda.csv', header=True)

print(lineups)
print(lineups.columns)
