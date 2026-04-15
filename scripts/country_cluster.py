import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "country_cluster.parquet")

duckdb.sql(f"""
                      SELECT Country, SUM(C0) C0, SUM(C1) C1, SUM(C2) C2, SUM(C3) C3, SUM(GK_C0) GK_C0, SUM(GK_C1) GK_C1, SUM(GK_C2) GK_C2
                      FROM (
                      SELECT distinct pl.*, 
                      CASE WHEN PosAdj != 'GK' AND mc.cluster = 0 THEN 1 ELSE 0 END AS C0,
                      CASE WHEN PosAdj != 'GK' AND mc.cluster = 1 THEN 1 ELSE 0 END AS C1,
                      CASE WHEN PosAdj != 'GK' AND mc.cluster = 2 THEN 1 ELSE 0 END AS C2,
                      CASE WHEN PosAdj != 'GK' AND mc.cluster = 3 THEN 1 ELSE 0 END AS C3,
                      CASE WHEN PosAdj = 'GK' AND gmc.cluster = 0 THEN 1 ELSE 0 END AS GK_C0,
                      CASE WHEN PosAdj = 'GK' AND gmc.cluster = 1 THEN 1 ELSE 0 END AS GK_C1,
                      CASE WHEN PosAdj = 'GK' AND gmc.cluster = 2 THEN 1 ELSE 0 END AS GK_C2
                      FROM (SELECT * FROM read_csv('{ADDITIONAL_DIR}/WC_Data/WC_COMBINED_PLAYERS.csv') WHERE IFNULL(Starter,'N') = 'Y') pl
                      LEFT JOIN (SELECT player_id, cluster
                                    FROM read_parquet('{ANALYSIS_DIR}/Mens_Clustering.parquet')) mc
                        ON pl.StatsbombID = mc.player_id
                      LEFT JOIN (SELECT player_id, cluster
                                    FROM read_parquet('{ANALYSIS_DIR}/Goal_Keeper_Clustering_Men.parquet')) gmc
                        ON pl.StatsbombID = gmc.player_id
                      
                      )
                      GROUP BY Country
                                """).write_parquet(output_path)

print('Country Cluster Done.')