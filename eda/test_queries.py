import duckdb
import math

#IF NOT INSTALLED THEN INSTALL spatial

project_location = 'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template'
#'C://Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/data'
#'C:/Users/Tyler/Documents/GitHub/soccer-analytics-capstone-template/eda'

# #test_query = 
# duckdb.sql(f"""
#                         SELECT *
#                         --season, competition, is_youth, is_international, country_name, season_name, COUNT(distinct match_id)
#                         --competition_stage, competition, match_status, match_week, home_team, home_team_id, home_managers, away_team_id, away_team, away_managers, stadium_id, referee_id, is_youth, is_international, country_name, season_name, COUNT(distinct match_id)
#                               FROM read_parquet('{project_location}/data/Statsbomb/matches.parquet') 
#                               WHERE match_id IN (SELECT match_id FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') )
#                               AND competition IN ('1. Bundesliga', 'Indian Super league', 'La Liga', 'Ligue 1', 'Serie A', 'Premier League')
#                         --GROUP BY season, competition, is_youth, is_international, country_name, season_name
#                         --ORDER BY season, competition, is_youth, is_international, country_name, season_name
#                         """
#                         ).write_csv('match_investigate.csv')
# #print(test_query)

# test_query2 = duckdb.sql(f"""
#                          with get_all_id as (
#                         SELECT distinct match_id, team_id, period, interval_start, GROUP_ATTRIBUTE, GROUP_NAME, UNIT_GROUPING_RANK_ID
#                          FROM read_parquet('{project_location}/eda/stack_lineup_groups.parquet')  st
#                          LEFT JOIN read_parquet('{project_location}/eda/unique_player_combos.parquet') pc
#                               ON IFNULL(st.position_1,-1) = IFNULL(pc.position_1,-1) 
#                               AND IFNULL(st.position_2,-1) = IFNULL(pc.position_2,-1) 
#                               AND IFNULL(st.position_3,-1) = IFNULL(pc.position_3,-1) 
#                               AND IFNULL(st.position_4,-1) = IFNULL(pc.position_4,-1) 
#                               AND IFNULL(st.position_5,-1) = IFNULL(pc.position_5,-1) 
#                               AND IFNULL(st.position_6,-1) = IFNULL(pc.position_6,-1) 
#                               AND IFNULL(st.position_7,-1) = IFNULL(pc.position_7,-1) 
#                               AND IFNULL(st.position_8,-1) = IFNULL(pc.position_8,-1) 
#                               AND IFNULL(st.position_9,-1) = IFNULL(pc.position_9,-1) 
#                               AND IFNULL(st.position_10,-1) = IFNULL(pc.position_10,-1) 
#                               AND IFNULL(st.position_11,-1) = IFNULL(pc.position_11,-1) 
#                         ),
#                         center_of_pitch as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID CENTER_POSITION_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Side' AND GROUP_ATTRIBUTE = 'C'
#                         ),
#                         left_of_pitch as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID LEFT_POSITION_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Side' AND GROUP_ATTRIBUTE = 'L'
#                         ),
#                         right_of_pitch as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID RIGHT_POSITION_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Side' AND GROUP_ATTRIBUTE = 'R'
#                         ),
#                         forwards as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID FORWARDS_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'F'
#                         ),
#                         midfielders as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID MIDFIELDERS_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'M'
#                         ),
#                         backs as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID BACKS_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'B'
#                         ),
#                         gk as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID GK_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Position Type' AND GROUP_ATTRIBUTE = 'GK'
#                         ),
#                         full_squad as (
#                         SELECT match_id, team_id, period, interval_start, UNIT_GROUPING_RANK_ID FULL_SQUAD_GROUPING_ID
#                         FROM get_all_id
#                         WHERE GROUP_NAME = 'Full Squad'                     
#                         )

#                         SELECT get_id.*, CENTER_POSITION_GROUPING_ID, LEFT_POSITION_GROUPING_ID, RIGHT_POSITION_GROUPING_ID, FORWARDS_GROUPING_ID, MIDFIELDERS_GROUPING_ID, BACKS_GROUPING_ID, GK_GROUPING_ID, FULL_SQUAD_GROUPING_ID
#                         FROM (SELECT distinct match_id, team_id, period, interval_start FROM get_all_id ) get_id
#                         LEFT JOIN center_of_pitch
#                               ON get_id.match_id = center_of_pitch.match_id
#                               AND get_id.team_id = center_of_pitch.team_id
#                               AND get_id.period = center_of_pitch.period
#                               AND get_id.interval_start = center_of_pitch.interval_start
#                         LEFT JOIN left_of_pitch
#                               ON get_id.match_id = left_of_pitch.match_id
#                               AND get_id.team_id = left_of_pitch.team_id
#                               AND get_id.period = left_of_pitch.period
#                               AND get_id.interval_start = left_of_pitch.interval_start
#                         LEFT JOIN right_of_pitch
#                               ON get_id.match_id = right_of_pitch.match_id
#                               AND get_id.team_id = right_of_pitch.team_id
#                               AND get_id.period = right_of_pitch.period
#                               AND get_id.interval_start = right_of_pitch.interval_start
#                         LEFT JOIN forwards
#                               ON get_id.match_id = forwards.match_id
#                               AND get_id.team_id = forwards.team_id
#                               AND get_id.period = forwards.period
#                               AND get_id.interval_start = forwards.interval_start
#                         LEFT JOIN midfielders
#                               ON get_id.match_id = midfielders.match_id
#                               AND get_id.team_id = midfielders.team_id
#                               AND get_id.period = midfielders.period
#                               AND get_id.interval_start = midfielders.interval_start
#                         LEFT JOIN backs
#                               ON get_id.match_id = backs.match_id
#                               AND get_id.team_id = backs.team_id
#                               AND get_id.period = backs.period
#                               AND get_id.interval_start = backs.interval_start
#                         LEFT JOIN gk
#                               ON get_id.match_id = gk.match_id
#                               AND get_id.team_id = gk.team_id
#                               AND get_id.period = gk.period
#                               AND get_id.interval_start = gk.interval_start
#                         LEFT JOIN full_squad
#                               ON get_id.match_id = full_squad.match_id
#                               AND get_id.team_id = full_squad.team_id
#                               AND get_id.period = full_squad.period
#                               AND get_id.interval_start = full_squad.interval_start
#                     """)

# print(test_query2)

#test_query3 = 
duckdb.sql(f"""

                        SELECT country_name, COUNT(distinct player_id) players, COUNT(distinct match_id) matches
                        FROM read_parquet('{project_location}/data/Statsbomb/lineups.parquet') 
                        GROUP BY country_name
                        

                    """).write_csv('player_composition.csv')

#print(test_query3)