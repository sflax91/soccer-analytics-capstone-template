from pathlib import Path

import streamlit as st
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.colors as colors

# Data paths
DATA_DIR = Path(__file__).parent.parent / "data"
STATSBOMB_DIR = DATA_DIR / "Statsbomb"
EDA_DIR = Path(__file__).parent.parent / "eda"
ANALYSIS_DIR = Path(__file__).parent.parent / "analysis"
POLYMARKET_DIR = DATA_DIR / "Polymarket"
ADDITIONAL_DIR = DATA_DIR / "Additional"

wc_2026_info = pd.read_csv(ADDITIONAL_DIR / "WC_Data/WC_2026.csv", index_col=False)
country_composition = pd.read_parquet(ADDITIONAL_DIR / "top_level_country_stats.parquet")
country_composition['join'] = country_composition['Country']
country_composition = country_composition.drop(columns=['Country'])
agg_country_xg = pl.read_parquet(ADDITIONAL_DIR / "agg_country_xg.parquet")
agg_country_time = pl.read_parquet(ADDITIONAL_DIR / "country_grouping.parquet")

cc = pl.read_parquet(ADDITIONAL_DIR / "country_cluster.parquet")

agg_country_time = agg_country_time.group_by("Country").agg(pl.col('grouping_minutes_on_pitch').sum())


agg_country_xg = agg_country_xg.join(agg_country_time, how='left', on='Country')

combine_info = pd.merge(wc_2026_info, country_composition, how='left', left_on='Country', right_on='join')

combine_info = combine_info.rename(columns={'BACKS':'Backs', 'MIDFIELDERS':'Midfielders', 'FORWARDS':'Forwards', 'Qualification_Route':'FIFA Confederation'})
combine_info = combine_info.rename(columns={'defensive_third_short_pct':'DT: Short',
                                            'defensive_third_patient_pct':'DT: Patient',
                                            'defensive_third_direct_pct':'DT: Direct',

                                            'attacking_third_short_pct':'AT: Short',
                                            'attacking_third_patient_pct':'AT: Patient',
                                            #'attacking_third_direct_pct':'AT: Direct %'

                                            'middle_third_short_pct':'MT: Short',
                                            'middle_third_patient_pct':'MT: Patient',
                                            'middle_third_direct_pct':'MT: Direct',
                                            
                                            })

combine_info['Backs'] = combine_info['Backs'].astype('Int32')
combine_info['Midfielders'] = combine_info['Midfielders'].astype('Int32')
combine_info['Forwards'] = combine_info['Forwards'].astype('Int32')

combine_info['DT: Short'] = round((combine_info['DT: Short'] * 100), 4).astype('string') + '%'
combine_info['DT: Patient'] = round((combine_info['DT: Patient'] * 100), 4).astype('string') + '%'
combine_info['DT: Direct'] = round((combine_info['DT: Direct'] * 100), 4).astype('string') + '%'

combine_info['MT: Short'] = round((combine_info['MT: Short'] * 100), 4).astype('string') + '%'
combine_info['MT: Patient'] = round((combine_info['MT: Patient'] * 100), 4).astype('string') + '%'
combine_info['MT: Direct'] = round((combine_info['MT: Direct'] * 100), 4).astype('string') + '%'

combine_info['AT: Short'] = round((combine_info['AT: Short'] * 100), 4).astype('string') + '%'
combine_info['AT: Patient'] = round((combine_info['AT: Patient'] * 100), 4).astype('string') + '%'
st.set_page_config(layout="wide")


country_one = 'United States'
focus_team = 'United States'
country_two = 'Canada'
opp_team = 'Canada'


Bin_Size = 2  # meters

@st.fragment
def plot_heat_map_display(df, value_col, team, title, team_one, cmap="coolwarm"):
    
    team_xg = df.filter(pl.col("Country") == team)
    #team_xg = df[df['Country'] == team]

    orientation = 'upper'
    if team_one == False:
        orientation = 'lower'


    team_xg = team_xg.with_columns([
    (pl.col("location_x_adj") // Bin_Size).cast(pl.Int64).alias("x_bin"),
    (pl.col("location_y_adj") // Bin_Size).cast(pl.Int64).alias("y_bin"),
    ])



    norm = colors.CenteredNorm(vcenter=0)

    shot_sequences_sorted = team_xg.sort(["x_bin", "y_bin"])
    shot_sequences_binned = shot_sequences_sorted.group_by(["x_bin", "y_bin", "grouping_minutes_on_pitch"]).agg([
        pl.col("shot").sum().alias("shots"),
        pl.col("shot_statsbomb_xg").sum().alias("total_xg"),
        pl.col("shot_statsbomb_xg").mean().alias("avg_xg"),
        ])
    #team_df = team_df.group_by(["x_bin", "y_bin"]).agg(pl.col(value_col).mean().alias(value_col))
    pandas_df = shot_sequences_binned.select(["x_bin", "y_bin", "grouping_minutes_on_pitch", value_col]).to_pandas()

    pandas_df['normalize_xG'] = pandas_df['total_xg'] / pandas_df['grouping_minutes_on_pitch']
    #pandas_df = pandas_df.set_index("")
    heat_map = (pandas_df.reset_index().pivot(index="x_bin", columns="y_bin", values='normalize_xG').fillna(0).values)
    plt.figure(figsize=(8,8))
    plt.imshow(heat_map, origin=orientation, aspect='auto',cmap=cmap, norm=norm)
    plt.colorbar(label='Total xG per Minute')
    plt.title(f"{title}")
    plt.xlabel("Pitch width (x bins)")
    plt.ylabel("Pitch length (y bins)")
    #plt.tight_layout()
    #plt.show()

    st.pyplot(plt)


@st.fragment
def updt_team_one(team_input):
    country_one = team_input

@st.fragment
def updt_team_two(team_input):
    country_two = team_input

@st.fragment
def refresh_comparison(focus_team, opp_team):
   

   one_cc = cc.filter(pl.col("Country") == focus_team)
   one_cc = one_cc.rename({'C0':'one_C0', 'C1':'one_C1', 'C2':'one_C2', 'C3':'one_C3', 'GK_C0':'one_GK_C0', 'GK_C1':'one_GK_C1', 'GK_C2':'one_GK_C2'})
   #one_stats = country_one_df.rename(columns={'Backs':'one_Backs', 'Midfielders':'one_Mi', 'C2':'one_C2', 'C3':'one_C3', 'GK_C0':'one_GK_C0', 'GK_C1':'one_GK_C1', 'GK_C2':'one_GK_C2'})
   
   #st.write(focus_team)
   #st.write(opp_team)


   two_cc = cc.filter(pl.col("Country") == opp_team)
   two_cc = two_cc.rename({'C0':'two_C0', 'C1':'two_C1', 'C2':'two_C2', 'C3':'two_C3', 'GK_C0':'two_GK_C0', 'GK_C1':'two_GK_C1', 'GK_C2':'two_GK_C2'})

   one_diff_c0 = one_cc['one_C0'][0] - two_cc['two_C0'][0]
   one_diff_c1 = one_cc['one_C1'][0] - two_cc['two_C1'][0]
   one_diff_c2 = one_cc['one_C2'][0] - two_cc['two_C2'][0]
   one_diff_c3 = one_cc['one_C3'][0] - two_cc['two_C3'][0]

   one_diff_gk_c0 = one_cc['one_GK_C0'][0] - two_cc['two_GK_C0'][0]
   one_diff_gk_c1 = one_cc['one_GK_C1'][0] - two_cc['two_GK_C1'][0]
   one_diff_gk_c2 = one_cc['one_GK_C2'][0] - two_cc['two_GK_C2'][0]
   
   one_info = combine_info[combine_info["join"] == focus_team]



   one_stats = combine_info[combine_info["join"] == focus_team]
   one_diff = pd.DataFrame(
       {
           'team_one': [focus_team],
           'DIFF_C0' : [one_diff_c0],
           'DIFF_C1' : [one_diff_c1],
           'DIFF_C2' : [one_diff_c2],
           'DIFF_C3' : [one_diff_c3],
           'DIFF_GK_C0' : [one_diff_c0],
           'DIFF_GK_C1' : [one_diff_c1],
           'DIFF_GK_C2' : [one_diff_c2],

           'DT: Short Poss %' : one_stats['DT: Short'].item().replace('%',''),
           'DT: Patient Poss %' : one_stats['DT: Patient'].item().replace('%',''),
           'MT: Short Poss %' : one_stats['MT: Short'].item().replace('%',''),
           'MT: Patient Poss %' : one_stats['MT: Patient'].item().replace('%',''),
           'MT: Direct Poss %' : one_stats['MT: Direct'].item().replace('%',''),
           'AT: Short Poss %' : one_stats['AT: Short'].item().replace('%',''),
           'AT: Patient Poss %' : one_stats['AT: Patient'].item().replace('%',''),
           'DT: Direct Poss %' : one_stats['DT: Direct'].item().replace('%','')

       },
           index=[0]
   )

   all_one = pd.merge(one_info, one_stats, how='left', left_on='Country', right_on='Country')
   

   
   all_one = pd.merge(one_info, one_diff, how='left', left_on='Country', right_on='team_one')
   all_one['exp_xG'] = (all_one['DIFF_C1'] * 0.070861) + (all_one['DIFF_GK_C1'] * 0.067093) + (all_one['DIFF_C0'] * 0.061063) + (all_one['DIFF_GK_C0'] * 0.054110) + (all_one['DIFF_C2'] * 0.024292) + (float(all_one['MT: Direct Poss %'][0]) * 0.022467) + (float(all_one['AT: Short Poss %'][0]) * 0.017636) + (90*0.017574) + (float(all_one['DT: Short Poss %'][0]) * 0.011664) + (float(all_one['DT: Direct Poss %'][0]) * 0.003539) + (float(all_one['DT: Patient Poss %'][0]) * -0.008289) + (float(all_one['MT: Patient Poss %'][0]) * -0.012605) - 0.023579 + (float(all_one['MT: Short Poss %'][0]) * -0.025619)
   
#    two_diff_c0 = two_cc['two_C0'][0] - one_cc['one_C0'][0]
#    two_diff_c1 = two_cc['two_C1'][0] - one_cc['one_C1'][0]
#    two_diff_c2 = two_cc['two_C2'][0] - one_cc['one_C2'][0]
#    two_diff_c3 = two_cc['two_C3'][0] - one_cc['one_C3'][0]
#    one_diff_gk_c0 = one_cc['one_GK_C0'][0] - two_cc['two_GK_C0'][0]
#    one_diff_gk_c1 = one_cc['one_GK_C1'][0] - two_cc['two_GK_C1'][0]
#    one_diff_gk_c2 = one_cc['one_GK_C2'][0] - two_cc['two_GK_C2'][0]
   
#    two_info = combine_info[combine_info["join"] == country_two]



#    two_stats = combine_info[combine_info["join"] == focus_team]
#    one_diff = pd.DataFrame(
#        {
#            'team_one': [focus_team],
#            'DIFF_C0' : [one_diff_c0],
#            'DIFF_C1' : [one_diff_c1],
#            'DIFF_C2' : [one_diff_c2],
#            'DIFF_C3' : [one_diff_c3],
#            'DIFF_GK_C0' : [one_diff_c0],
#            'DIFF_GK_C1' : [one_diff_c1],
#            'DIFF_GK_C2' : [one_diff_c2],

#            'DT: Short Poss %' : one_stats['DT: Short'].item().replace('%',''),
#            'DT: Patient Poss %' : one_stats['DT: Patient'].item().replace('%',''),
#            'MT: Short Poss %' : one_stats['MT: Short'].item(),
#            'MT: Patient Poss %' : one_stats['MT: Patient'].item(),
#            'MT: Direct Poss %' : one_stats['MT: Direct'].item().replace('%',''),
#            'AT: Short Poss %' : one_stats['AT: Short'].item(),
#            'AT: Patient Poss %' : one_stats['AT: Patient'].item(),
#            'DT: Direct Poss %' : one_stats['DT: Direct'].item()

#        },
#            index=[0]
#    )

#    all_two = pd.merge(two_info, two_stats, how='left', left_on='Country', right_on='Country')
   

   
#    all_two = pd.merge(two_info, one_diff, how='left', left_on='Country', right_on='team_one')
#    all_two['exp_xG'] = all_one['DIFF_C1'] * 0.070861 + all_one['DIFF_GK_C1'] * 0.067093+ all_one['DIFF_C0'] * 0.061063 + all_one['DIFF_GK_C0'] * 0.054110 + all_one['DIFF_C2'] * 0.024292 #+ float(all_one['MT: Direct Poss %'][0]) * 0.022467 #+ float(all_one['AT: Short Poss %'][0]) * 0.017636
   





   st.table(all_one[['DT: Short Poss %', 'DT: Patient Poss %', 'DT: Direct Poss %', 'MT: Short Poss %', 'MT: Patient Poss %', 'MT: Direct Poss %', 'AT: Short Poss %', 'AT: Patient Poss %', 'exp_xG']], hide_index=True)
   



st.title('2026 World Cup Comparison Tool')

col1, col2, col3 = st.columns([.475,.05,.475])

with col1:
    country_one = st.selectbox("Select a Team:", options=sorted(wc_2026_info['Country'].unique()), key="team_one_select")
    country_one_df = combine_info[combine_info["join"] == country_one]
    st.table(country_one_df[['Group', 'FIFA Confederation', 'Manager', 'Backs', 'Midfielders', 'Forwards']], hide_index=True)

    #updt_team_one(country_one)
    #updt_team_two(country_two)
    #st.table(country_one_df[['DT: Short', 'DT: Patient', 'DT: Direct', 'MT: Short', 'MT: Patient', 'MT: Direct', 'AT: Short', 'AT: Patient']], hide_index=True)
    refresh_comparison(country_one, country_two)
    plot_heat_map_display(agg_country_xg, "total_xg", country_one, "Offense at Top", True)




with col3:
    country_two = st.selectbox("Select an Opponent:", options=sorted(wc_2026_info['Country'].unique()), key="team_two_select")
    country_two_df = combine_info[combine_info["join"] == country_two]
    st.table(country_two_df[['Group', 'FIFA Confederation', 'Manager', 'Backs', 'Midfielders', 'Forwards']], hide_index=True)

    #st.table(country_two_df[['DT: Short', 'DT: Patient', 'DT: Direct', 'MT: Short', 'MT: Patient', 'MT: Direct', 'AT: Short', 'AT: Patient']], hide_index=True)

    refresh_comparison(country_two, country_one)

    plot_heat_map_display(agg_country_xg, "total_xg", country_two, "Defense at Top", False)
