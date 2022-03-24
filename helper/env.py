import os

import streamlit as st


class Env(object):

    @staticmethod
    def init():
        return list(st.secrets.keys())

    @staticmethod
    def get(environment, key):
        if key in environment:
            return environment[key]
        raise KeyError(f'{key} not found in environment variables')

    @staticmethod
    def USER_AGENT():
        return Env.get(st.secrets, 'USER_AGENT')

    @staticmethod
    def REDDIT_CLIENT_ID():
        return Env.get(st.secrets, 'REDDIT_CLIENT_ID')

    @staticmethod
    def REDDIT_CLIENT_SECRET():
        return Env.get(st.secrets, 'REDDIT_CLIENT_SECRET')

    @staticmethod
    def KAGGLE_USERNAME():
        return Env.get(st.secrets, 'KAGGLE_USERNAME')

    @staticmethod
    def KAGGLE_KEY():
        return Env.get(st.secrets, 'KAGGLE_KEY')

    @staticmethod
    def VSCODE_WORKSPACE():
        return Env.get(os.environ, 'VSCODE_WORKSPACE')


# init on import
Env.init()
