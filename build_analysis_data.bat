@echo off
python %~dp0Scripts\pass.py && python %~dp0Scripts\carry.py && python %~dp0Scripts\possession_types.py && python %~dp0Scripts\position_type.py  && python %~dp0Scripts\match_score_timeline.py

echo Group 1 Done.

python %~dp0Scripts\period_lineups.py && python %~dp0Scripts\player_match_on_pitch.py && python %~dp0Scripts\event_proximity.py

echo Group 2 Done.

python %~dp0Scripts\player_match_timeline_with_score.py && python %~dp0Scripts\pass_level_stats.py && python %~dp0Scripts\shot_level_stats.py

del "%~dp0data\Additional\match_score_timeline.parquet"

echo Group 3 Done.

python %~dp0Scripts\team_formation_timeline.py && python %~dp0Scripts\stack_lineup_groups.py && python %~dp0Scripts\team_groupings_timeline.py && python %~dp0Scripts\team_composition.py

del "%~dp0data\Additional\team_formation_timeline.parquet"
del "%~dp0data\Additional\team_groupings_timeline.parquet"
del "%~dp0data\Additional\stack_lineup_groups.parquet"

echo Group 4 Done.

python %~dp0Scripts\gk_stats.py && python %~dp0Scripts\gk_k_means.py

echo Group 5 Done.

python %~dp0Scripts\pass_k_means.py 

del "%~dp0data\Additional\pass.parquet"
del "%~dp0data\Additional\pass_level_stats.parquet"

python %~dp0Scripts\shot_k_means.py

del "%~dp0data\Additional\shot_level_stats.parquet"

python %~dp0Scripts\carry_k_means.py

del "%~dp0data\Additional\carry.parquet"


python %~dp0Scripts\defense_k_means.py
python %~dp0Scripts\all_k_means.py


del "%~dp0data\Additional\player_match_timeline_with_score.parquet"
del "%~dp0data\Additional\gk_stats.parquet"
del "%~dp0data\Additional\player_match_on_pitch.parquet"
del "%~dp0data\Additional\event_proximity.parquet"
del "%~dp0data\Additional\pass_k_means.parquet"
del "%~dp0data\Additional\shot_k_means.parquet"
del "%~dp0data\Additional\carry_k_means.parquet"
del "%~dp0data\Additional\defense_k_means.parquet"
del "%~dp0data\Additional\gk_k_means.parquet"


python %~dp0Scripts\period_lineups_adv.py && python %~dp0Scripts\for_regression.py

