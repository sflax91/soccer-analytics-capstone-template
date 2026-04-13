import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "carry_level_stats.parquet")

duckdb.sql(f"""
                      with carry_events as (
                        SELECT match_id, id, index_num, period, --timestamp, 
                      duration, possession, possession_team_id, team_id, 
                         player_id,
                      strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_date
                        FROM read_parquet('{STATSBOMB_DIR}/events.parquet')
                        WHERE carry_end_location_x IS NOT NULL
                        ),
                        get_score as (
                        SELECT carry_events.*, EVENT_ZONE_START, EVENT_ZONE_END, off_team.TEAM_COMPOSITION_PK OFF_TEAM_COMPOSITION_PK, def_team.TEAM_COMPOSITION_PK DEF_TEAM_COMPOSITION_PK,
                        home_away.HOME_AWAY OFF_HOME_AWAY, home_away2.team_id DEF_TEAM_ID, home_away2.HOME_AWAY DEF_HOME_AWAY,
                        score_check1.home_score,
                        score_check1.away_score,
                        off_team.PLAYERS_ON_PITCH - def_team.PLAYERS_ON_PITCH player_advantage
                        FROM carry_events
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/event_proximity.parquet') ep
                              ON carry_events.match_id = ep.match_id
                              AND carry_events.id = ep.id
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') off_team
                          ON carry_events.match_id = off_team.match_id
                          AND carry_events.team_id = off_team.team_id
                          AND carry_events.period = off_team.period
                          AND carry_events.event_date >= off_team.interval_start
                          AND carry_events.event_date < off_team.interval_end
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') def_team
                          ON carry_events.match_id = def_team.match_id
                          AND carry_events.team_id != def_team.team_id
                          AND carry_events.period = def_team.period
                          AND carry_events.event_date >= def_team.interval_start
                          AND carry_events.event_date < def_team.interval_end
                        LEFT JOIN (                        
                                    SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

                                    UNION

                                    SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
                                    ) home_away
                           ON carry_events.match_id = home_away.match_id 
                           AND carry_events.team_id = home_away.team_id 
                          LEFT JOIN (                        
                                    SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

                                    UNION

                                    SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
                                    ) home_away2
                           ON carry_events.match_id = home_away2.match_id 
                           AND carry_events.team_id != home_away2.team_id 

                           LEFT JOIN (
                                    SELECT match_id, period, home_goals home_score, away_goals away_score, 
                                    start_date SCORE_TL_START,
                                    end_date SCORE_TL_END
                                    FROM read_parquet('{ADDITIONAL_DIR}/match_score_timeline.parquet')
                                    ) score_check1
                            ON carry_events.match_id = score_check1.match_id
                            AND carry_events.period = score_check1.period
                            AND carry_events.event_date >= score_check1.SCORE_TL_START
                            AND carry_events.event_date < score_check1.SCORE_TL_END


                        )

                        SELECT get_score.*, 
                        CASE WHEN OFF_HOME_AWAY = 'H' THEN home_score - away_score ELSE away_score - home_score END AS goal_diff,
                        FROM get_score
                    """).write_parquet(output_path)

print('Carry Level Stats Done.')