
# %% IMPORT
import os
import json
import warnings

import numpy as np
import pandas as pd

import altair as alt
import streamlit as st


# %% SETTINGS
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 20)


# %% CONFIG
with open(os.path.join('config', 'config.json')) as f:
    config = json.load(f)
title = f'Reddit - r/{config["subreddit"]}'
st.set_page_config(layout='wide', initial_sidebar_state='expanded', page_title=title)
st.title(title)


# %% INTRO
st.markdown('TODO')


# %% LOAD DATA
df = pd.read_hdf(os.path.join('data', config['subreddit'], config['reddit']['data']['submission']))


# %% VISUALIZE DATA
st.title('Data')

dt_keys = sorted(df.dtypes.unique().astype(str))
dt = st.multiselect('Types', dt_keys, default=dt_keys)

if not dt:
    st.warning('Select datatypes to analyze.')
else:
    st.dataframe(df.select_dtypes(include=dt).dtypes, height=600)

    st.write('Data')
    st.dataframe(df.select_dtypes(include=dt).tail(20), height=600)


# %% ANALYZE TODO
st.title('TODO')
st.sidebar.title('TODO')
