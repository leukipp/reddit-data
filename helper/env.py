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
    def PUBLISH():
        return Env._get(os.environ, 'PUBLISH')

    @staticmethod
    def PAUSE():
        return Env._get(os.environ, 'PAUSE')

    @staticmethod
    def VSCODE_WORKSPACE():
        return Env._get(os.environ, 'VSCODE_WORKSPACE')

    def __repr__(self):
        return '\n'.join([f'{x} = {getattr(Env, x)()}' for x in dir(Env) if callable(getattr(Env, x)) and not x.startswith('_')])


# init on import
Env()
