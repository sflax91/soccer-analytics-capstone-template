import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "stack_lineup_groups.parquet")

duckdb.sql(f"""
                        with get_ranks as (
                        SELECT team_id, match_id, period, interval_start, interval_end, player_id, --country_id, 
                        --POSITION_SIDE_ADJ, 
                        POSITION_TYPE_ALT, POSITION_BEHAVIOR, POSITION_TYPE,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ ORDER BY match_id, team_id, period, interval_start, POSITION_SIDE_ADJ, player_id) PLAYER_POSITION_SIDE_ADJ_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE, player_id) PLAYER_POSITION_TYPE_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT ORDER BY match_id, team_id, period, interval_start, POSITION_TYPE_ALT, player_id) PLAYER_POSITION_TYPE_ALT_ID_RANK,
                        --RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, country_id ORDER BY match_id, team_id, period, interval_start, country_id, player_id) PLAYER_COUNTRY_ID_RANK,
                        RANK() OVER (PARTITION BY match_id, team_id, period, interval_start, interval_end ORDER BY match_id, team_id, period, interval_start, interval_end, player_id) PLAYER_SQUAD_RANK
                        
                        FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet') 
                        ),
                        agg_ranks as (
                        SELECT gr.team_id,  gr.match_id, gr.period, gr.interval_start, gr.interval_end, gr.player_id, --gr.country_id, 
                        --gr.POSITION_SIDE_ADJ, 
                        gr.POSITION_TYPE, gr.POSITION_TYPE_ALT, --gr.PLAYER_POSITION_SIDE_ADJ_ID_RANK, 
                        gr.POSITION_BEHAVIOR, gr.PLAYER_POSITION_TYPE_ALT_ID_RANK, --gr.PLAYER_COUNTRY_ID_RANK, 
                        gr.PLAYER_SQUAD_RANK, gr.PLAYER_POSITION_TYPE_ID_RANK--,
                        --SUM(CASE WHEN gr.country_id = gr2.country_id THEN 1 ELSE 0 END) PLAYERS_SAME_COUNTRY,
                        --SUM(CASE WHEN gr.country_id != gr2.country_id THEN 1 ELSE 0 END) PLAYERS_DIFF_COUNTRY,
                        --SUM(CASE WHEN gr.POSITION_TYPE = gr2.POSITION_TYPE AND gr.country_id = gr2.country_id THEN 1 ELSE 0 END) POSITION_SAME_COUNTRY,
                        --SUM(CASE WHEN gr.POSITION_TYPE = gr2.POSITION_TYPE AND gr.country_id != gr2.country_id THEN 1 ELSE 0 END) POSITION_DIFF_COUNTRY,
                        --SUM(CASE WHEN gr.POSITION_SIDE_ADJ = gr2.POSITION_SIDE_ADJ AND gr.country_id = gr2.country_id THEN 1 ELSE 0 END) SIDE_SAME_COUNTRY,
                        --SUM(CASE WHEN gr.POSITION_SIDE_ADJ = gr2.POSITION_SIDE_ADJ AND gr.country_id != gr2.country_id THEN 1 ELSE 0 END) SIDE_DIFF_COUNTRY

                        FROM get_ranks gr
                        LEFT JOIN get_ranks gr2
                              ON gr.team_id = gr2.team_id
                              AND gr.match_id = gr2.match_id
                              AND gr.period = gr2.period
                              AND gr.interval_start = gr2.interval_start
                              AND gr.interval_end = gr2.interval_end
                              AND gr.player_id != gr2.player_id
                        GROUP BY gr.team_id,  gr.match_id, gr.period, gr.interval_start, gr.interval_end, gr.player_id, --gr.country_id, 
                        --gr.POSITION_SIDE_ADJ, 
                        gr.POSITION_TYPE, gr.POSITION_TYPE_ALT, 
                        --gr.PLAYER_POSITION_SIDE_ADJ_ID_RANK, 
                        gr.POSITION_BEHAVIOR, gr.PLAYER_POSITION_TYPE_ALT_ID_RANK, --gr.PLAYER_COUNTRY_ID_RANK, 
                        gr.PLAYER_SQUAD_RANK, gr.PLAYER_POSITION_TYPE_ID_RANK
           
                        ), 
                        find_position_type as (
                        SELECT team_id, match_id, period, interval_start, interval_end, player_id, PLAYER_POSITION_TYPE_ID_RANK, POSITION_TYPE
                         FROM agg_ranks
                         ),
                        group_position_type as (
                        PIVOT (
                        SELECT distinct match_id, team_id, period, interval_start, interval_end, player_id, POSITION_TYPE,
                        'position_' || CAST(PLAYER_POSITION_TYPE_ID_RANK as varchar) PLAYER_ID_RANK
                        FROM find_position_type
                              )
                              ON PLAYER_ID_RANK
                              USING MIN(player_id)
                              GROUP BY match_id, team_id, period, interval_start, interval_end, POSITION_TYPE
                        ),
                        find_squad as (
                        SELECT team_id, match_id, period, interval_start, interval_end, player_id, PLAYER_SQUAD_RANK
                         FROM agg_ranks
                         ), 
                         group_squad as (
                        PIVOT (
                        SELECT distinct match_id, team_id, period, interval_start, interval_end, player_id,
                        'position_' || CAST(PLAYER_SQUAD_RANK as varchar) PLAYER_ID_RANK
                        FROM find_squad
                              )
                              ON PLAYER_ID_RANK
                              USING MIN(player_id)
                              GROUP BY match_id, team_id, period, interval_start, interval_end
                        ),
                         --find_country as (
                        --SELECT team_id, match_id, period, interval_start, interval_end, player_id--, PLAYER_COUNTRY_ID_RANK--, country_id
                        -- FROM agg_ranks                        
                        -- ),
                        -- group_country as (
                        --PIVOT (
                        --SELECT distinct match_id, team_id, period, interval_start, interval_end, player_id, --country_id,
                        --'position_' || CAST(PLAYER_COUNTRY_ID_RANK as varchar) PLAYER_ID_RANK
                        --FROM find_country
                        --      )
                         --     ON PLAYER_ID_RANK
                         --     USING MIN(player_id)
                         --     GROUP BY match_id, team_id, period, interval_start, interval_end--, country_id
                       -- ),
                         --find_position_type_alt as (
                        --SELECT team_id, match_id, period, interval_start, interval_end, player_id, PLAYER_POSITION_TYPE_ALT_ID_RANK, POSITION_TYPE_ALT
                         --FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet') 
                         --),
                         --group_position_type_alt as (

                        --PIVOT (
                        --SELECT distinct match_id, team_id, period, interval_start, interval_end, player_id, POSITION_TYPE_ALT,
                        --'position_' || CAST(PLAYER_POSITION_TYPE_ALT_ID_RANK as varchar) PLAYER_ID_RANK
                       --FROM find_position_type_alt
                        --      )
                        --      ON PLAYER_ID_RANK
                        --      USING MIN(player_id)
                         --     GROUP BY match_id, team_id, period, interval_start, interval_end, POSITION_TYPE_ALT
                        --),
                         find_position_side as (
                        SELECT team_id, match_id, period, interval_start, interval_end, player_id--, PLAYER_POSITION_SIDE_ADJ_ID_RANK, POSITION_SIDE_ADJ
                         FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet') 
                         ),
                        --group_position_side as (
--
 --                       PIVOT (
   --                     SELECT distinct match_id, team_id, period, interval_start, interval_end, player_id, POSITION_SIDE_ADJ,
    --                    'position_' || CAST(PLAYER_POSITION_SIDE_ADJ_ID_RANK as varchar) PLAYER_ID_RANK
    --                    FROM find_position_side
      --                        )
      --                        ON PLAYER_ID_RANK
      --                        USING MIN(player_id)
       --                       GROUP BY match_id, team_id, period, interval_start, interval_end, POSITION_SIDE_ADJ
                        
            --            ),
                        stack_groups as (
                        SELECT match_id, team_id, period, interval_start, interval_end, POSITION_TYPE GROUP_ATTRIBUTE, 'Position Type' GROUP_NAME,
                        position_1, position_2, position_3, position_4, position_5, position_6, NULL position_7, NULL position_8, NULL position_9, NULL position_10, NULL position_11
                        FROM group_position_type
                        
                        UNION

                        SELECT match_id, team_id, period, interval_start, interval_end, 'Full Squad' GROUP_ATTRIBUTE, 'Full Squad' GROUP_NAME,
                        position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11
                        FROM group_squad

                        --UNION

                        --SELECT match_id, team_id, period, interval_start, interval_end, CAST(country_id as varchar) GROUP_ATTRIBUTE, 'Country' GROUP_NAME,
                        --position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, position_10, position_11
                        --FROM group_country

                        --UNION

                        --SELECT match_id, team_id, period, interval_start, interval_end, POSITION_TYPE_ALT GROUP_ATTRIBUTE, 'Position Type Alt' GROUP_NAME,
                        --position_1, position_2, position_3, position_4, position_5, position_6, NULL position_7, NULL position_8, NULL position_9, NULL position_10, NULL position_11
                        --FROM group_position_type_alt

                        --UNION

                        --SELECT match_id, team_id, period, interval_start, interval_end, POSITION_SIDE_ADJ GROUP_ATTRIBUTE, 'Position Side' GROUP_NAME,
                        --position_1, position_2, position_3, position_4, position_5, position_6, position_7, position_8, position_9, NULL position_10, NULL position_11
                        --FROM group_position_side
                        )

                        SELECT *
                        FROM stack_groups


      

                    """).write_parquet(output_path)
print('Stack Lineup Groups Done.')