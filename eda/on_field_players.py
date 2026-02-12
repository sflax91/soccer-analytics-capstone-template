import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'
project_location2 = 'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'


match_facts = duckdb.sql(f"""
                         SELECT e.*, player_id
                         FROM (
                         SELECT id, index_num, period, minute, second, match_id, type
                         FROM read_parquet('{project_location2}/Statsbomb/events.parquet') 
                         WHERE match_id = 7582
                         ) e
                         LEFT JOIN read_parquet('{project_location}/period_lineups.parquet')  pl
                           ON e.match_id = pl.match_id
                           AND e.period = pl.period
                           AND ((e.minute = pl.end_minutes AND e.second <= pl.end_seconds)
                                 OR e.minute < pl.end_minutes
                                 )
                        WHERE player_id IS NULL
                        ORDER BY e.match_id, e.period, e.minute, e.second, player_id
                    """)#.write_csv('lineups_eda.csv', header=True)

print(match_facts)
print(match_facts.columns)


other_query = duckdb.sql(f"""
                         SELECT *
                         FROM read_parquet('{project_location}/period_lineups.parquet')  pl
                         WHERE match_id = 7582 --AND period = 4
                        ORDER BY match_id, period DESC, player_id
                    """)#.write_csv('lineups_eda.csv', header=True)

print(other_query)