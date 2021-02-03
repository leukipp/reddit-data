
# %% IMPORT
import os
import json
import warnings

import numpy as np
import pandas as pd

import altair as alt
import streamlit as st


# %% FUNCTIONS
@st.cache
def load(path): return pd.read_hdf(path)


# %% SETTINGS
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 20)


# %% CONFIG
with open(os.path.join('config', 'config.json')) as f:
    config = json.load(f)
title = f'Reddit - r/{config["subreddit"]}'
st.set_page_config(layout='wide', initial_sidebar_state='expanded', page_title=title)


# %% INTRO
st.title(title)

st.markdown('TODO')


# %% LOAD DATA
st.sidebar.title('Data')

df = load(os.path.join('data', config['subreddit'], config['reddit']['data']['submission']))
created_min = df.iloc[0]['created'].replace(hour=0, minute=0, second=0).to_pydatetime()
created_max = df.iloc[-1]['created'].replace(hour=0, minute=0, second=0).to_pydatetime()

st.sidebar.header('Sample')
sample = st.sidebar.slider('Size', value=10000, min_value=10, max_value=df.shape[0])

st.sidebar.header('Filter')
created = st.sidebar.slider('Created', value=[created_min, created_max], min_value=created_min, max_value=created_max, format='Y-MM-DD')

df_filtered = df[(df['created'] >= created[0]) & (df['created'] <= created[1])]
df_sampled = df_filtered.sample(frac=1, random_state=42).head(sample)


# %% VISUALIZE DATA
st.title('Data')

dt_keys = sorted(df.dtypes.unique().astype(str))
dt = st.multiselect('Types', dt_keys, default=dt_keys)

if not dt:
    st.warning('Select datatypes to analyze.')
else:
    st.dataframe(df.select_dtypes(include=dt).dtypes, height=600)

    st.write('Data')
    st.dataframe(df_filtered.select_dtypes(include=dt).tail(20), height=600)


# %% ANALYZE COUNT
st.title('Count')

labels = ['pinned', 'archived', 'locked', 'removed', 'deleted', 'is_self', 'is_video', 'is_original_content']
highlight = st.selectbox('Highlight', labels, index=3, key='Highlight_1')
maxbins = st.slider('Maxbins', value=25, min_value=10, max_value=40, key='Maxbins_1')

chart = alt.Chart(df_sampled).mark_bar().encode(
    alt.X('created:T', bin=alt.Bin(maxbins=maxbins), axis=alt.Axis(format='%Y-%b-%d', labelOverlap=False, labelAngle=-90)),
    alt.Y('count()'),
    alt.Color(f'{highlight}:N')
).properties(
    height=500
).interactive()
st.altair_chart(chart, use_container_width=True)
