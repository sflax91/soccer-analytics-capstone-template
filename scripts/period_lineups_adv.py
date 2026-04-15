import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
output_path = str(ADDITIONAL_DIR / "period_lineups_adv.parquet")

duckdb.sql(f"""
                        
                        with agg_attributes as (
           
                        SELECT pl.match_id, team_id, period, interval_start, interval_end, gender MEN_WOMEN,
                        SUM(CASE WHEN POSITION_TYPE = 'B' THEN 1 ELSE 0 END) AS BACKS,
                        SUM(CASE WHEN POSITION_TYPE = 'M' THEN 1 ELSE 0 END) AS MIDFIELDERS,
                        SUM(CASE WHEN POSITION_TYPE = 'F' THEN 1 ELSE 0 END) AS FORWARDS,
                        SUM(CASE WHEN POSITION_TYPE = 'GK' THEN 1 ELSE 0 END) AS GOALKEEPER, COUNT(distinct pl.player_id) PLAYERS_ON_PITCH,

                        SUM(CASE WHEN IFNULL(mc.cluster, wc.cluster) = 0 THEN 1 ELSE 0 END) AS C0,
                        SUM(CASE WHEN IFNULL(mc.cluster, wc.cluster) = 1 THEN 1 ELSE 0 END) AS C1,
                        SUM(CASE WHEN IFNULL(mc.cluster, wc.cluster) = 2 THEN 1 ELSE 0 END) AS C2,
                        SUM(CASE WHEN IFNULL(mc.cluster, wc.cluster) = 3 THEN 1 ELSE 0 END) AS C3,
                        SUM(CASE WHEN IFNULL(mc.cluster, wc.cluster) = 4 THEN 1 ELSE 0 END) AS C4,
                        SUM(CASE WHEN IFNULL(mc.cluster, wc.cluster) = 5 THEN 1 ELSE 0 END) AS C5,


                        SUM(CASE WHEN mc_gk.cluster = 0 THEN 1 ELSE 0 END) AS GK_C0,
                        SUM(CASE WHEN mc_gk.cluster = 1 THEN 1 ELSE 0 END) AS GK_C1,
                        SUM(CASE WHEN mc_gk.cluster = 2 THEN 1 ELSE 0 END) AS GK_C2
           

                        FROM read_parquet('{ADDITIONAL_DIR}/period_lineups.parquet') pl
                        LEFT JOIN read_parquet('{STATSBOMB_DIR}/matches.parquet') m
                            ON pl.match_id = m.match_id
                        LEFT JOIN (SELECT distinct player_id, cluster FROM read_parquet('{ANALYSIS_DIR}/Mens_Clustering.parquet')) mc
                            ON gender = 'male' AND POSITION_TYPE != 'GK'
                            AND pl.player_id = mc.player_id
                        LEFT JOIN (SELECT distinct gk_player_id, cluster FROM read_parquet('{ANALYSIS_DIR}/Goal_Keeper_Clustering_Men.parquet')) mc_gk
                            ON gender = 'male' AND POSITION_TYPE = 'GK'
                            AND pl.player_id = mc_gk.gk_player_id
                        LEFT JOIN (SELECT distinct player_id, cluster FROM read_parquet('{ANALYSIS_DIR}/Womens_Clustering.parquet')) wc
                            ON gender = 'female'
                            AND pl.player_id = wc.player_id
                        WHERE pl.match_id != 3900519
                        GROUP BY pl.match_id, team_id, period, interval_start, interval_end, gender
                        )

                        SELECT match_id, team_id, period, interval_start, interval_end,
                        CASE 
                        WHEN TRIM(MEN_WOMEN) = 'male' THEN 'M' 
                        WHEN TRIM(MEN_WOMEN) = 'female' THEN 'W' 
                        ELSE NULL 
                        END AS MEN_WOMEN, 
                        BACKS, MIDFIELDERS, FORWARDS, GOALKEEPER, C0, C1, C2, C3, C4, C5, GK_C0, GK_C1, GK_C2, PLAYERS_ON_PITCH, 
                        RANK() OVER (ORDER BY match_id, team_id, period, interval_start) GROUPING_PK
                        FROM agg_attributes
                        
                    """).write_parquet(output_path)
print('Period Lineups Adv Done.')