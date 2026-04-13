import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "defense_k_means.parquet")

duckdb.sql(f"""
                      with derive_stats as (
                      SELECT e.match_id, player_id,
                      CASE WHEN IFNULL(duel_outcome, duel_type) LIKE 'Lost%' THEN 1 ELSE 0 END AS duel_lost,
                      CASE 
                      WHEN duel_outcome LIKE 'Success%' THEN 1 
                      WHEN duel_outcome = 'Won' THEN 1 
                      ELSE 0 
                      END AS duel_won,
                      CASE WHEN foul_committed_type IS NOT NULL THEN 1 ELSE 0 END AS foul_committed,
                      CASE WHEN foul_committed_card LIKE '%Yellow%' THEN 1 ELSE 0 END as yellow_card,
                      CASE WHEN foul_committed_card = 'Red' THEN 1 ELSE 0 END as red_card,
                      CASE WHEN e.possession_team_id != e.team_id AND type = 'Pressure' THEN 1 ELSE 0 END as pressure_applied,
                      CASE WHEN e.possession_team_id != e.team_id AND type = 'Block' THEN 1 ELSE 0 END AS blocks,
                      CASE 
                      WHEN e.possession_team_id != e.team_id AND interception_outcome = 'Won' THEN 1 
                      WHEN e.possession_team_id != e.team_id AND interception_outcome LIKE 'Success%' THEN 1 
                      ELSE 0 END AS interceptions
                      --,
                      --SUBSTR(EVENT_ZONE_START,1,2) pitch_zone_vertical
                      FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                      --LEFT JOIN read_parquet('{ADDITIONAL_DIR}/event_proximity.parquet') ep
                        --ON e.id = ep.id
                        --AND e.match_id = ep.match_id
                      WHERE duel_type IS NOT NULL OR type IN ('Dribbled Past','Duel', 'Foul Committed', 'Interception', 'Pressure', 'Block')
                      ),
                      match_level as (
                      SELECT match_id, player_id, SUM(duel_lost) duel_lost, SUM(duel_won) duel_won, SUM(foul_committed) foul_committed,
                        SUM(yellow_card) yellow_card, SUM(red_card) red_card, SUM(pressure_applied) pressure_applied, SUM(blocks) blocks, SUM(interceptions) interceptions
                      FROM derive_stats
                      GROUP BY match_id, player_id
                      ),
                      get_player_time as (
                      SELECT match_level.player_id, SUM(duel_lost) duel_lost, SUM(duel_won) duel_won, SUM(foul_committed) foul_committed,
                        SUM(yellow_card) yellow_card, SUM(red_card) red_card, SUM(pressure_applied) pressure_applied, 
                        SUM(blocks) blocks, SUM(interceptions) interceptions, SUM(MINUTES_ON_PITCH) MINUTES_ON_PITCH
                      FROM match_level
                      LEFT JOIN read_parquet('{ADDITIONAL_DIR}/player_match_on_pitch.parquet') pt
                        ON match_level.match_id = pt.match_id
                        AND match_level.player_id = pt.player_id
                      GROUP BY match_level.player_id
                      )

                      SELECT player_id, 
                      CASE 
                      WHEN duel_won = 0 THEN 0
                      WHEN (duel_won + duel_lost) = 0 THEN 0
                      ELSE duel_won / (duel_won + duel_lost) END AS pct_duel_won, 
                      CASE
                      WHEN (duel_won + duel_lost) = 0 THEN 0
                      ELSE (duel_won + duel_lost) / MINUTES_ON_PITCH END AS duels_per_minute,
                      foul_committed / MINUTES_ON_PITCH foul_committed_per_minute,
                      yellow_card / MINUTES_ON_PITCH yellow_cards_per_minute,
                      red_card / MINUTES_ON_PITCH red_cards_per_minute,
                      pressure_applied / MINUTES_ON_PITCH pressures_applied_per_minute,
                      blocks / MINUTES_ON_PITCH blocks_per_minute,
                      interceptions / MINUTES_ON_PITCH interceptions_per_minute
                      FROM get_player_time
                    """).write_parquet(output_path)
print('Defense K Means Done.')