import duckdb
from pathlib import Path

EDA_DIR = Path(__file__).parent.parent / "eda"
DATA_DIR = Path(__file__).parent.parent / "data"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
ADDITIONAL_DIR = DATA_DIR / "Additional"
output_path = str(ADDITIONAL_DIR / "shot_level_stats.parquet")

#x coords
#0-18 left box
#102-120 right box

#y coords
#40 +- (20.115)
#60.115
#19.885


#left box

#top left
#(60.115, 0)
#top right
#(60.115, 18)
#bottom left
#(19.885, 0)
#bottom right
#(19.885, 18)


#right box

#top left
#(60.115, 102)
#top right
#(60.115, 120)
#bottom left
#(19.885, 102)
#bottom right
#(19.885, 120)

#halfway 60

duckdb.sql(f"""
                      with shot_info as (
                        SELECT e.match_id, --e.id, 
                         --strptime('2026-01-01' , '%Y-%m-%d') start_date,
                         --minute, second,
                        strptime('2026-01-01' , '%Y-%m-%d') + TO_MINUTES(minute) + TO_SECONDS(second) event_date,
                         e.period, e.team_id, e.possession, --location_x, location_y, 
                          --e.possession_team_id, --timestamp, duration, --location_x, location_y, 
                         player_id, --shot_end_location_x, shot_end_location_y, shot_end_location_z, 
                        shot_statsbomb_xg, shot_outcome, 
                        shot_technique, 
                         shot_body_part, shot_type, 
                         CASE WHEN shot_first_time THEN 1 ELSE 0 END AS shot_first_time, 
                         CASE WHEN shot_deflected THEN 1 ELSE 0 END AS shot_deflected, 
                         CASE WHEN shot_aerial_won THEN 1 ELSE 0 END AS shot_aerial_won, 
                         CASE WHEN shot_follows_dribble THEN 1 ELSE 0 END AS shot_follows_dribble, 
                         CASE WHEN shot_one_on_one THEN 1 ELSE 0 END AS shot_one_on_one,
                         CASE WHEN shot_open_goal THEN 1 ELSE 0 END AS shot_open_goal, 
                         CASE WHEN shot_redirect THEN 1 ELSE 0 END AS shot_redirect, 
                         CASE WHEN shot_saved_off_target THEN 1 ELSE 0 END AS shot_saved_off_target, 
                         CASE WHEN shot_saved_to_post THEN 1 ELSE 0 END AS shot_saved_to_post, 

                        EVENT_ZONE_START OFF_ZONE, DIST_TO_GOAL
                      
                        FROM read_parquet('{STATSBOMB_DIR}/events.parquet') e
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/event_proximity.parquet') ep
                           ON e.match_id = ep.match_id
                           AND e.id = ep.id
                           AND e.period = ep.period
                          LEFT JOIN read_parquet('{STATSBOMB_DIR}/matches.parquet') m
                            ON e.match_id = m.match_id
                        WHERE shot_end_location_x IS NOT NULL
                        --AND e.match_id = 15956
                        ),
                        get_score as (
                        SELECT shot_info.*, off_team.TEAM_COMPOSITION_PK OFF_TEAM_COMPOSITION_PK, def_team.TEAM_COMPOSITION_PK DEF_TEAM_COMPOSITION_PK,
                        home_away.HOME_AWAY OFF_HOME_AWAY, home_away2.team_id DEF_TEAM_ID, home_away2.HOME_AWAY DEF_HOME_AWAY,
                        score_check1.home_score,
                        score_check1.away_score,
                        off_team.PLAYERS_ON_PITCH - def_team.PLAYERS_ON_PITCH player_advantage
                        FROM shot_info
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') off_team
                          ON shot_info.match_id = off_team.match_id
                          AND shot_info.team_id = off_team.team_id
                          AND shot_info.period = off_team.period
                          AND shot_info.event_date >= off_team.interval_start
                          AND shot_info.event_date < off_team.interval_end
                        LEFT JOIN read_parquet('{ADDITIONAL_DIR}/team_composition.parquet') def_team
                          ON shot_info.match_id = def_team.match_id
                          AND shot_info.team_id != def_team.team_id
                          AND shot_info.period = def_team.period
                          AND shot_info.event_date >= def_team.interval_start
                          AND shot_info.event_date < def_team.interval_end
                        LEFT JOIN (                        
                                    SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

                                    UNION

                                    SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
                                    ) home_away
                           ON shot_info.match_id = home_away.match_id 
                           AND shot_info.team_id = home_away.team_id 
                          LEFT JOIN (                        
                                    SELECT match_id, home_team_id team_id, 'H' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')

                                    UNION

                                    SELECT match_id, away_team_id team_id, 'A' HOME_AWAY
                                    FROM read_parquet('{STATSBOMB_DIR}/matches.parquet')
                                    ) home_away2
                           ON shot_info.match_id = home_away2.match_id 
                           AND shot_info.team_id != home_away2.team_id 

                           LEFT JOIN (
                                    SELECT match_id, period, home_goals home_score, away_goals away_score, 
                                    start_date SCORE_TL_START,
                                    end_date SCORE_TL_END
                                    FROM read_parquet('{ADDITIONAL_DIR}/match_score_timeline.parquet')
                                    ) score_check1
                            ON shot_info.match_id = score_check1.match_id
                            AND shot_info.period = score_check1.period
                            AND shot_info.event_date >= score_check1.SCORE_TL_START
                            AND shot_info.event_date < score_check1.SCORE_TL_END


                        )

                        SELECT get_score.*, 
                        CASE WHEN OFF_HOME_AWAY = 'H' THEN home_score - away_score ELSE away_score - home_score END AS goal_diff,
                        FROM get_score
                    """).write_parquet(output_path)
print('Shot Level Stats Done.')