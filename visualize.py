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

from kaggle.api.kaggle_api_extended import KaggleApi


# %% FUNCTIONS
@st.cache(ttl=60 * 60)
def download_dataset(dataset):
    kaggle = KaggleApi()
    kaggle.authenticate()
    path = tempfile.mkdtemp()
    kaggle.dataset_download_files(dataset, path=path, quiet=False, force=True, unzip=True)
    return path


@st.cache(ttl=60 * 60)
def read_dataset(path, file_name):
    return pd.read_hdf(os.path.join(path, file_name))


# %% SETTINGS
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 20)


# %% CONFIG
dataset = None
with open(os.path.join('data', 'public', 'dataset-metadata.json')) as f:
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
df = read_dataset(download_dataset(dataset), file_name=os.path.join(subreddit, 'submissions_reddit.h5'))

st.sidebar.header('Filter')

# filter created
created_min = df.iloc[0]['created'].replace(hour=0, minute=0, second=0).to_pydatetime()
created_max = df.iloc[-1]['created'].replace(hour=0, minute=0, second=0).to_pydatetime()
created = st.sidebar.slider('Created', value=[created_min, created_max], min_value=created_min, max_value=created_max, format='Y-MM-DD')
df_filtered = df[(df['created'] >= created[0]) & (df['created'] <= created[1])]

# filter flags
flags = ['pinned', 'archived', 'locked', 'removed', 'deleted', 'is_self', 'is_video', 'is_original_content']
exclude = st.sidebar.multiselect('Exclude', flags, default=[])
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
    st.dataframe(df.select_dtypes(include=dt).dtypes, height=600)

    st.write('Data')
    st.dataframe(df_filtered.select_dtypes(include=dt).tail(20), height=600)


# %% ANALYZE COUNT
st.title('Count')

highlight = st.selectbox('Highlight', flags, index=3)
maxbins = st.slider('Maxbins', value=(created_max - created_min).days, min_value=1, max_value=100)

chart = alt.Chart(df_sampled).mark_bar().encode(
    alt.X('created:T', bin=alt.Bin(maxbins=maxbins), axis=alt.Axis(format='%Y-%b-%d', labelOverlap=False, labelAngle=-90)),
    alt.Y('count()'),
    alt.Color(f'{highlight}:N')
).properties(
    height=500
).interactive()
st.altair_chart(chart, use_container_width=True)
