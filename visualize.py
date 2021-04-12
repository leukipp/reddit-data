# %% IMPORTS
import os
import re
import json
import tempfile

import glob as gb
import numpy as np
import pandas as pd
import yfinance as yf

import nltk as nl
import altair as alt
import streamlit as st
import plotly.express as px
import plotly.graph_objs as go

from wordcloud import WordCloud
from collections import Counter
from common.kaggle import Kaggle


# %% INITIALIZATION
pd.options.plotting.backend = 'plotly'


# %% FUNCTIONS
@st.cache(ttl=60*60*1)
def download_nltk(data):
    download_dir = tempfile.mkdtemp()
    nl.download(data, download_dir=download_dir)
    return download_dir


@st.cache(ttl=60*60*1)
def download_dataset(dataset):
    return Kaggle().download(dataset, local=False)


@st.cache(ttl=60*60*1)
def read_dataset(path, file_name):
    return pd.read_hdf(os.path.join(path, file_name))


# %% CONFIG
dataset, title = '', ''
with open(os.path.join('data', 'public', 'datapackage.json')) as f:
    d = json.load(f)
    dataset, title = d['id'], d['title']

subreddits = []
for path in sorted(gb.glob(os.path.join('config', '*.json'))):
    with open(path) as f:
        d = json.load(f)
        subreddits.append(d['subreddit'])

st.set_page_config(layout='wide', initial_sidebar_state='expanded', page_title=title)


# %% LOAD DATA
st.sidebar.title('Data')

st.sidebar.header('Dataset')
subreddit = st.sidebar.selectbox('Subreddit', subreddits, index=subreddits.index('wallstreetbets'))

nl.data.path.append(download_nltk(['stopwords']))
df = read_dataset(download_dataset(dataset), file_name=os.path.join(subreddit, f'submissions_reddit.h5'))

# -- FILTER --
st.sidebar.header('Filter')

# filter created
created_min = df['created'].min().replace(hour=0, minute=0, second=0).to_pydatetime()
created_max = df['created'].max().replace(hour=23, minute=59, second=59).to_pydatetime()
created = st.sidebar.slider('Created', value=[created_min, created_max], min_value=created_min, max_value=created_max, format='Y-MM-DD')
df_filtered = df[(df['created'] >= created[0]) & (df['created'] <= created[1])]

# filter flags
flags = ['pinned', 'archived', 'locked', 'removed', 'deleted', 'is_self', 'is_video', 'is_original_content']
exclude = st.sidebar.multiselect('Exclude', flags, default=['removed', 'deleted'])
for e in exclude:
    df_filtered = df_filtered[df_filtered[e] == 0]

if not df_filtered.shape[0]:
    st.warning('No data available!')
    st.stop()

# -- SAMPLE --
st.sidebar.header('Sample')

# sample size
sample = st.sidebar.slider('Size', value=min(10000, df_filtered.shape[0]), min_value=1, max_value=df_filtered.shape[0])
df_sampled = df_filtered.sample(frac=1, random_state=42).head(sample)

if not df_sampled.shape[0]:
    st.warning('No data available!')
    st.stop()


# %% INTRO
st.title(f'Reddit - r/{subreddit}')

st.markdown(f'Reddit submissions from Finance/Investment/Stock related posts: https://www.kaggle.com/{dataset}')


# %% VISUALIZE DATA
st.title('Data')

dt_keys = sorted(df.dtypes.unique().astype(str))
dt = st.multiselect('Types', dt_keys, default=dt_keys)

if not dt:
    st.warning('Select datatypes to analyze!')
else:
    search = st.text_input('Search', '')
    if search.strip():
        search_mask = [False] * df_filtered.shape[0]
        for s in search.split(','):
            if s.strip():
                for column in ['author', 'link_flair_text', 'title', 'selftext']:
                    search_mask = search_mask | df_filtered['author'].str.contains(s.strip(), case=False)
        df_filtered = df_filtered.loc[search_mask]

    df_dtypes = df_filtered.select_dtypes(include=dt)
    df_dtypes_size = len(df.select_dtypes(include=dt).dtypes) / 2

    head = max(int(np.floor(df_dtypes_size)), 0)
    tail = max(int(np.ceil(df_dtypes_size)), 1)

    col_left, col_right = st.beta_columns((1, 3))
    col_left.dataframe(df.select_dtypes(include=dt).dtypes, height=600)
    col_right.dataframe(df_dtypes.head(head).append(df_dtypes.tail(tail)), height=600)

    st.write(f'Total: {df_dtypes.shape[0]}')


# %% ANALYZE SUBMISSIONS
st.title('Submissions')

highlight = st.selectbox('Highlight', flags, index=5)

chart = alt.Chart(df_sampled).mark_bar().encode(
    alt.X('utcyearmonthdate(created):T', bin=True, axis=alt.Axis(format='%Y-%b-%d', labelAngle=-90)),
    alt.Y('count()'),
    alt.Color(f'{highlight}:N')
).properties(
    height=500
).interactive()
st.altair_chart(chart, use_container_width=True)


# %% ANALYZE WORDS
st.title('Words')

topwords = 50
stopwords = nl.corpus.stopwords.words('english')

args = {
    'background_color': 'white',
    'colormap': 'plasma',
    'min_word_length': 3,
    'max_words': 400,
    'height': 310,
    'width': 460,
    'scale': 1.5
}

wordcloud = WordCloud(stopwords=stopwords, **args)
text = re.sub(r'http\S+', '', ''.join(df_sampled['title'].to_list() + df_sampled['selftext'].to_list()))

frequencies = wordcloud.process_text(text)
wordcloud.generate_from_frequencies(frequencies)
words = np.array(sorted(frequencies.items(), key=lambda x: x[1], reverse=True))

stopwords = stopwords + st.multiselect('Exclude', words[:topwords, 0])

frequencies = {k: frequencies[k] for k in words[:, 0] if k not in stopwords}
wordcloud.generate_from_frequencies(frequencies)
words = pd.DataFrame(frequencies.items(), columns=['word', 'count'])

image = wordcloud.to_image()
treemap = px.treemap(words.head(topwords), path=['word'], values='count')
treemap.update_layout(margin=dict(t=0, b=0, r=0, l=0))

col_left, col_right = st.beta_columns((1, 1))
col_left.plotly_chart(treemap, use_container_width=True)
col_right.image(image, use_column_width=True)


# %% ANALYZE STOCKS
st.title('Stocks')

tickers = open(os.path.join('data', 'tickers.csv')).read().splitlines()
tickers = words[words['word'].apply(lambda x: any(y for y in tickers if y == x))].head(10).to_dict('records')

if len(tickers):
    ticker = st.selectbox('Ticker', tickers, format_func=lambda x: f'{x["word"]} (#{x["count"]})', index=0)['word']

    start, end = created[0].strftime('%Y-%m-%d'), created[1].strftime('%Y-%m-%d')
    prices = yf.download(ticker, start=start, end=end, interval='1d', prepost=True)

    args = {
        'open': prices['Open'],
        'low': prices['Low'],
        'high': prices['High'],
        'close': prices['Close']
    }

    candlestick = go.Figure(data=[go.Candlestick(x=[x.strftime('%Y-%m-%d %H:%M') for x in prices.index], **args)])
    candlestick.update_layout(
        height=500,
        yaxis_title=ticker,
        margin=dict(t=0, b=0, r=0, l=0),
        annotations=[dict(x=start, y=0.05, xref='x', yref='paper', showarrow=False, xanchor='left', text=' ')]
    )

    st.plotly_chart(candlestick, use_container_width=True)
else:
    st.warning('No ticker symbols found!')
