import duckdb
#import pandas as pd


project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data/Statsbomb'

con = duckdb.connect()

#result_tuples = con.execute(f"SELECT * FROM read_parquet('C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data/Statsbomb/events.parquet') LIMIT 100").fetchall()
#print(result_tuples)

events = duckdb.sql(f"SELECT type, COUNT(*) FROM read_parquet('{project_location}/events.parquet') WHERE match_id = 15973 GROUP BY type LIMIT 100")
#print(events.limit.0)
print(events.columns)

lineups = duckdb.sql(f"SELECT * FROM read_parquet('{project_location}/lineups.parquet') WHERE match_id = 15973 LIMIT 100")
#print(lineups)
#print(lineups.columns)


matches = duckdb.sql(f"SELECT * FROM read_parquet('{project_location}/matches.parquet') WHERE match_id = 15973 LIMIT 100")
#print(matches)
#print(matches.columns)


events_subset = duckdb.sql(f"SELECT id, index_num, period, minute, second, timestamp, CAST(IFNULL(string_split(timestamp, '.')[2],'000') AS INTEGER) ts_millisecond, duration FROM read_parquet('{project_location}/events.parquet') WHERE match_id = 15973 LIMIT 100")
#print(events_subset)
#print(events_subset.columns)

# match_timeline = duckdb.sql(f"""SELECT *
#                                 FROM (
#                                         SELECT * 
#                                         FROM read_parquet('{project_location}/events.parquet') 
#                                         WHERE match_id = 15973 
#                                     ) e

#                             """
                                        
                                        
                                        
#                                         )

# print(match_timeline)
# print(match_timeline.columns)


# duckdb.sql(f"""SELECT type, COUNT(*) 
#                 FROM read_parquet('{project_location}/events.parquet')
#                 WHERE match_id = 15973 
#                 GROUP BY type 
#                 LIMIT 100""").show(max_rows=100)

# duckdb.sql(f"""SELECT *
#                 FROM read_parquet('{project_location}/events.parquet')
#                 WHERE match_id = 15973 AND type = 'Substitution'
#                 LIMIT 100""").show(max_rows=100, max_col_width=10000)

subs = duckdb.sql(f"""SELECT player_id, substitution_replacement_id
                 FROM read_parquet('{project_location}/events.parquet')
                 WHERE match_id = 15973 AND type = 'Substitution'
                 LIMIT 100""")

print(subs)
print(subs.columns)