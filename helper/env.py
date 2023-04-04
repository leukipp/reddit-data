import os

import streamlit as st


class Env(object):
    def __init__(self):
        list(st.secrets.keys())

    @staticmethod
    def _get(environment, key):
        if key in environment:
            return environment[key]
        return None

    @staticmethod
    def USER_AGENT():
        return Env._get(st.secrets, 'USER_AGENT')

    @staticmethod
    def REDDIT_CLIENT_ID():
        return Env._get(st.secrets, 'REDDIT_CLIENT_ID')

    @staticmethod
    def REDDIT_CLIENT_SECRET():
        return Env._get(st.secrets, 'REDDIT_CLIENT_SECRET')

    @staticmethod
    def KAGGLE_USERNAME():
        return Env._get(st.secrets, 'KAGGLE_USERNAME')

    @staticmethod
    def KAGGLE_KEY():
        return Env._get(st.secrets, 'KAGGLE_KEY')

    @staticmethod
    def SUBREDDITS():
        return Env._get(os.environ, 'SUBREDDITS')

    @staticmethod
    def DATA():
        return Env._get(os.environ, 'DATA') or os.path.join('data')

    @staticmethod
    def LOADER():
        return Env._get(os.environ, 'LOADER') or os.path.join('config', 'loader.json')

    @staticmethod
    def KAGGLE():
        return Env._get(os.environ, 'KAGGLE') or os.path.join('config', 'kaggle.json')

    @staticmethod
    def BACKGROUND():
        return Env._get(os.environ, 'BACKGROUND') or False

    @staticmethod
    def PUBLISH():
        return Env._get(os.environ, 'PUBLISH') or False

    @staticmethod
    def PAUSE():
        return Env._get(os.environ, 'PAUSE') or 0

    def __repr__(self):
        return '\n'.join([f'{x} = {getattr(Env, x)()}' for x in dir(Env) if callable(getattr(Env, x)) and not x.startswith('_')])


# init on import
Env()
