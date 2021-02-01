import os
import praw
import json
import base64
import getpass
import simplecrypt

import glob as gb
import pandas as pd

from tqdm import tqdm
from datetime import datetime


class Reddit(object):
    def __init__(self, global_config, reddit_config):
        self.global_config = global_config
        self.reddit_config = reddit_config

        # load global config
        with open(global_config) as f:
            config = json.load(f)
            self.subreddit = config['subreddit']
            self.data = config['reddit']['data']

        # load private config
        config = self.read_config()
        while not config:
            self.write_config()
            config = self.read_config()
        self.client = praw.Reddit(**{**config, **{'user_agent': 'python:https://github.com/leukipp/TODO:v0.0.1 (by /u/leukipp)'}})

    def read_config(self):
        try:
            # read config
            print('\ndecrypting reddit config')
            with open(self.reddit_config) as f:
                config = json.load(f)
                config['client_secret'] = simplecrypt.decrypt(self.reddit_config, base64.b64decode(config['client_secret'])).decode('utf8')
                return config
        except:
            return {}

    def write_config(self):
        print('\nupdate reddit config')
        client_id = input('enter client_id: ').strip()
        client_secret = getpass.getpass('enter client_secret: ').strip()

        # write config
        print('\nencrypting reddit config')
        with open(self.reddit_config, 'w') as f:
            config = {
                'client_id': client_id,
                'client_secret': base64.b64encode(simplecrypt.encrypt(self.reddit_config, client_secret)).decode('utf-8')
            }
            json.dump(config, f, indent=4, sort_keys=True)

    def run(self):
        folder = os.path.join('data', self.subreddit)
        os.makedirs(folder, exist_ok=True)

        # download reddit data by metadata
        metadata = {os.path.basename(x): pd.read_csv(x, delimiter=';') for x in gb.glob(os.path.join(folder, '*.csv'))}
        for file_type, file_path in self.data.items():
            self.download(metadata, file_type, os.path.join(folder, file_path))

    def download(self, metadata, file_type, file_path):
        columns = [
            'id', 'author', 'created', 'retrieved', 'edited',
            'gilded', 'pinned', 'archived', 'locked',
            'removed', 'deleted',
            'is_self', 'is_video', 'is_original_content',
            'title', 'link_flair_text', 'upvote_ratio', 'score',
            'total_awards_received', 'num_comments', 'num_crossposts',
            'selftext', 'thumbnail', 'shortlink'
        ]

        # load data
        data = []
        for key in metadata:
            df_metadata = metadata[key].drop_duplicates(file_type, keep='last')

            # process only submissions for now
            if file_type == 'submission':

                # add t3_ prefix for submission fullnames
                ids = ('t3_' + df_metadata[file_type]).tolist()[:200]

                # fetch data
                data = data + self.fetch(file_type, ids)

        # export submissions
        df = pd.DataFrame(data=data, columns=columns).drop_duplicates('id', keep='last')
        df.to_hdf(file_path, key='df', mode='w', complevel=9)
        print(f'\nexported {df.shape[0]} {file_type}s')

    def fetch(self, file_type, ids):
        data = []

        # chunk ids into lists with size 100
        print(f'\ndownload {len(ids)} {file_type}s\n')
        for fullnames in tqdm([ids[i:i + 100] for i in range(0, len(ids), 100)], desc='fetching', unit_scale=100):
            now = datetime.utcnow().timestamp()

            # process only submissions for now
            if file_type == 'submission':
                submissions = self.client.info(fullnames=fullnames)

                # submission data
                data = data + [[
                    str(s.id), str(s.author), int(s.created_utc), int(now), int(s.edited),
                    int(s.gilded), int(s.pinned), int(s.archived), int(s.locked),
                    int(s.selftext == '[removed]' or s.removed_by_category != None), int(s.selftext == '[deleted]'),
                    int(s.is_self), int(s.is_video), int(s.is_original_content),
                    str(s.title), str(s.link_flair_text), float(s.upvote_ratio), int(s.score),
                    int(s.total_awards_received), int(s.num_comments), int(s.num_crossposts),
                    str(s.selftext), str(s.thumbnail), str(s.shortlink)
                ] for s in submissions]

        return data
