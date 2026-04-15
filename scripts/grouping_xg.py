import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "grouping_xg.parquet")

duckdb.sql(f"""
                      with event_data as (
                      SELECT id, location_x, location_y, match_id, period, possession, shot_statsbomb_xg, possession_team_id, strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_timestamp,
                      CASE WHEN shot_outcome IS NOT NULL THEN 1 ELSE 0 END AS shot
                      FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                      WHERE IFNULL(shot_statsbomb_xg,0) > 0 OR shot_outcome IS NOT NULL
                      ),
                      get_groupings as (
                      SELECT event_data.id, location_x, location_y, off_lineup.GROUPING_PK OFF_GROUPING_PK, def_lineup.GROUPING_PK DEF_GROUPING_PK, shot_statsbomb_xg, shot
                      
                      FROM event_data
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') off_lineup
                        ON event_data.match_id = off_lineup.match_id
                        AND event_data.period = off_lineup.period
                        AND event_data.possession_team_id = off_lineup.team_id
                        AND event_timestamp >= off_lineup.interval_start
                        AND event_timestamp < IFNULL(off_lineup.interval_end, CURRENT_TIMESTAMP)
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/period_lineups_adv.parquet') def_lineup
                        ON event_data.match_id = def_lineup.match_id
                        AND event_data.period = def_lineup.period
                        AND event_data.possession_team_id != def_lineup.team_id
                        AND event_timestamp >= def_lineup.interval_start
                        AND event_timestamp < IFNULL(def_lineup.interval_end, CURRENT_TIMESTAMP)

                        ),

                        stack_results as (
                        SELECT OFF_GROUPING_PK GROUPING_PK, shot_statsbomb_xg, shot,
                        CASE WHEN location_x > 60 THEN 120 - location_x ELSE location_x END AS location_x_adj,
                        CASE WHEN location_x > 60 THEN 80 - location_y ELSE location_y END AS location_y_adj,
                        'Offense' STATS_GROUP, id
                        FROM get_groupings

                        UNION
                      
                        SELECT DEF_GROUPING_PK, -shot_statsbomb_xg, shot,
                        CASE WHEN location_x < 60 THEN 120 - location_x ELSE location_x END AS location_x_adj,
                        CASE WHEN location_x < 60 THEN 80 - location_y ELSE location_y END AS location_y_adj,
                        'Defense' STATS_GROUP, id
                        FROM get_groupings
                        )

                        SELECT stack_results.*
                        FROM stack_results





                                """).write_parquet(output_path)

print('Grouping xG Done.')