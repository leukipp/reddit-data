# %% IMPORT
import os
import json
import tempfile
import warnings

import glob as gb
import numpy as np
import pandas as pd

import altair as alt
import streamlit as st

from common.kaggle import Kaggle


# %% FUNCTIONS
@st.cache(ttl=60*60)
def download_dataset(dataset):
    return Kaggle().download(dataset)


@st.cache(ttl=60*60)
def read_dataset(path, file_name):
    return pd.read_hdf(os.path.join(path, file_name))


# %% CONFIG
dataset = None
with open(os.path.join('data', 'public', 'datapackage.json')) as f:
    dataset = json.load(f)['id']

subreddits = []
for path in sorted(gb.glob(os.path.join('config', '*.json'))):
    with open(path) as f:
        subreddits.append(json.load(f)['subreddit'])

st.set_page_config(layout='wide', initial_sidebar_state='expanded', page_title=f'Kaggle · {dataset}')

# %% LOAD DATA
st.sidebar.title('Data')

st.sidebar.header('Dataset')
subreddit = st.sidebar.selectbox('Subreddit', subreddits, index=11)

# download dataset
df = read_dataset(download_dataset(dataset), file_name=os.path.join(subreddit, f'submissions_reddit.h5'))

st.sidebar.header('Filter')

# filter created
created_min = df['created'].min().replace(hour=0, minute=0, second=0).to_pydatetime()
created_max = df['created'].max().replace(hour=0, minute=0, second=0).to_pydatetime()
created = st.sidebar.slider('Created', value=[created_min, created_max], min_value=created_min, max_value=created_max, format='Y-MM-DD')
df_filtered = df[(df['created'] >= created[0]) & (df['created'] <= created[1])]

# filter flags
flags = ['pinned', 'archived', 'locked', 'removed', 'deleted', 'is_self', 'is_video', 'is_original_content']
exclude = st.sidebar.multiselect('Exclude', flags, default=['removed', 'deleted'])
for e in exclude:
    df_filtered = df_filtered[df_filtered[e] == 0]

st.sidebar.header('Sample')

# sample size
sample = st.sidebar.slider('Size', value=min(10000, df_filtered.shape[0]), min_value=1, max_value=df_filtered.shape[0])
df_sampled = df_filtered.sample(frac=1, random_state=42).head(sample)


# %% INTRO
st.title(f'Reddit - r/{subreddit}')

st.markdown(f'TODO: https://www.kaggle.com/{dataset}')


# %% VISUALIZE DATA
st.title('Data')

dt_keys = sorted(df.dtypes.unique().astype(str))
dt = st.multiselect('Types', dt_keys, default=dt_keys)

if not dt:
    st.warning('Select datatypes to analyze.')
else:
    df_dtypes = df_filtered.select_dtypes(include=dt)
    df_dtypes_size = len(df.select_dtypes(include=dt).dtypes) / 2

    col_left, col_right = st.beta_columns((1, 3))
    col_left.dataframe(df.select_dtypes(include=dt).dtypes, height=600)
    col_right.dataframe(df_dtypes.head(int(np.floor(df_dtypes_size))).append(df_dtypes.tail(int(np.ceil(df_dtypes_size)))), height=600)


# %% ANALYZE COUNT
st.title('Count')

highlight = st.selectbox('Highlight', flags, index=5)

chart = alt.Chart(df_sampled).mark_bar().encode(
    alt.X('utcyearmonthdate(created):T', bin=True, axis=alt.Axis(format='%Y-%b-%d',  labelAngle=-90)),
    alt.Y('count()'),
    alt.Color(f'{highlight}:N')
).properties(
    height=500
).interactive()
st.altair_chart(chart, use_container_width=True)
