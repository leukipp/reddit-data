
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
st.set_page_config(layout='wide', initial_sidebar_state='expanded', page_title='Reddit-Yolo')


# %% GENREAL INFORMATION
st.title('Reddit-Yolo')
with open(os.path.join('config', 'config.json')) as f:
    config = json.load(f)


# %% LOAD DATA
df = pd.read_hdf(os.path.join('data', config['subreddit'], config['reddit']['data']['submission']))


# %% VISUALIZE DATA
dt_keys = sorted(df.dtypes.unique().astype(str))
dt = st.multiselect('Types', dt_keys, default=dt_keys)

if not dt:
    st.warning('Select datatypes to analyze.')
else:
    st.write('Data')
    st.dataframe(df.select_dtypes(include=dt).head(20))

    st.write('Types')
    st.dataframe(df.select_dtypes(include=dt).dtypes, height=600)


# %% VISUALIZE TODO
st.title('TODO')
st.sidebar.title('TODO')
