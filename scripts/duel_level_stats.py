import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "duel_level_stats.parquet")

duckdb.sql(f"""
                  with duel_events as (
                     SELECT match_id, id, index_num, period, timestamp, duration, possession, possession_team_id, team_id, 
                         player_id, 
                        CASE WHEN duel_type = 'Tackle' THEN 1 ELSE 0 END AS tackles, 
                        CASE WHEN duel_type = 'Aerial Lost' THEN 1 ELSE 0 END AS aerial_lost, 
           
                      CASE WHEN duel_outcome IN ('Lost In Play', 'Lost Out') OR duel_type = 'Aerial Lost' THEN 1 ELSE 0 END AS duels_lost,
                      CASE WHEN duel_outcome IN ('Success In Play', 'Success Out', 'Won') THEN 1 ELSE 0 END AS duels_won,


                      strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_date
                        FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
                        WHERE duel_type IS NOT NULL
                      ),
                        get_score as (
                        SELECT duel_events.*, EVENT_ZONE_START, EVENT_ZONE_END, off_team.TEAM_COMPOSITION_PK OFF_TEAM_COMPOSITION_PK, def_team.TEAM_COMPOSITION_PK DEF_TEAM_COMPOSITION_PK,
                        home_away.HOME_AWAY OFF_HOME_AWAY, home_away2.team_id DEF_TEAM_ID, home_away2.HOME_AWAY DEF_HOME_AWAY,
                        score_check1.home_score,
                        score_check1.away_score,
                        off_team.PLAYERS_ON_PITCH - def_team.PLAYERS_ON_PITCH player_advantage
                        FROM duel_events
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/event_proximity.parquet') ep
                              ON duel_events.match_id = ep.match_id
                              AND duel_events.id = ep.id
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') off_team
                          ON duel_events.match_id = off_team.match_id
                          AND duel_events.team_id = off_team.team_id
                          AND duel_events.period = off_team.period
                          AND duel_events.event_date >= off_team.interval_start
                          AND duel_events.event_date < off_team.interval_end
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') def_team
                          ON duel_events.match_id = def_team.match_id
                          AND duel_events.team_id != def_team.team_id
                          AND duel_events.period = def_team.period
                          AND duel_events.event_date >= def_team.interval_start
                          AND duel_events.event_date < def_team.interval_end
                        LEFT JOIN (                        
                                    SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

                                    UNION

                                    SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
                                    ) home_away
                           ON duel_events.match_id = home_away.match_id 
                           AND duel_events.team_id = home_away.team_id 
                          LEFT JOIN (                        
                                    SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

                                    UNION

                                    SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
                                    ) home_away2
                           ON duel_events.match_id = home_away2.match_id 
                           AND duel_events.team_id != home_away2.team_id 

                           LEFT JOIN (
                                    SELECT match_id, period, home_goals home_score, away_goals away_score, 
                                    start_date SCORE_TL_START,
                                    end_date SCORE_TL_END
                                    FROM read_parquet('{ADDITIONAL_DIR}/match_score_timeline.parquet')
                                    ) score_check1
                            ON duel_events.match_id = score_check1.match_id
                            AND duel_events.period = score_check1.period
                            AND duel_events.event_date >= score_check1.SCORE_TL_START
                            AND duel_events.event_date < score_check1.SCORE_TL_END

                        )

                        SELECT get_score.*, 
                        CASE WHEN OFF_HOME_AWAY = 'H' THEN home_score - away_score ELSE away_score - home_score END AS goal_diff,
                        FROM get_score
                    """).write_parquet(output_path)
print('Duel Level Stats Done.')