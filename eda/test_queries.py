import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

test_query = duckdb.sql(f"""

                         with half_timestamps as (
                              SELECT distinct match_id, period, minute, second, timestamp
                                    FROM read_parquet('{project_location}/Statsbomb/events.parquet') 
                                    WHERE type IN ('Half End', 'Half Start')
                                    
                         ),
                         
                         all_start_end as (
                         SELECT *
                         FROM ( SELECT *
                         FROM (
                           SELECT match_id, period, minute, second, timestamp, 
                              LEAD(minute,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, minute, second) period_end_minute, 
                              LEAD(second,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, minute, second) period_end_second, 
                              LEAD(timestamp,1) OVER (PARTITION BY match_id, period ORDER BY match_id, period, minute, second) period_end_timestamp
                            FROM half_timestamps
                            )
                        )
                        WHERE period_end_timestamp IS NOT NULL
                        )

                        SELECT *
                        FROM all_start_end
                        WHERE match_id = 7582
                                    

                        
                    """)

print(test_query)


test_query2 = duckdb.sql(f"""
                                 SELECT match_id, MAX(period) last_period
                                 FROM read_parquet('{project_location}/Statsbomb/events.parquet')
                                 WHERE match_id = 7582 
                                 GROUP BY match_id
                        
                                    

                        
                    """)

print(test_query2)
