import os
import sys
import praw
import json
import base64
import getpass
import simplecrypt

import glob as gb
import numpy as np
import pandas as pd

from tqdm import tqdm
from datetime import datetime, timezone

root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # nopep8
sys.path.insert(0, root)  # nopep8

from common.sleep import Sleep
from common.loader import Loader


class Reddit(Loader):
    def __init__(self, root, global_config, reddit_config):
        Loader.__init__(self, name='Reddit')

        self.root = root
        self.global_config = os.path.join(self.root, global_config)
        self.reddit_config = os.path.join(self.root, reddit_config)

        # load global config
        with open(global_config) as f:
            config = json.load(f)
            self.subreddit = config['subreddit']
            self.data = config['reddit']['data']
            self.periode = config['reddit']['periode']

        # load private config
        config = self.read_config()
        while not config:
            self.write_config()
            config = self.read_config()
        self.client = praw.Reddit(**{**config, **{'user_agent': 'python:https://github.com/leukipp/TODO:v0.0.1 (by /u/leukipp)'}})

    def read_config(self):
        try:
            # read config
            self.log('decrypting reddit config')
            with open(self.reddit_config) as f:
                config = json.load(f)
                config['client_secret'] = simplecrypt.decrypt(self.reddit_config, base64.b64decode(config['client_secret'])).decode('utf8')
                return config
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            return {}

    def write_config(self):
        self.log('update reddit config')
        client_id = input(f'{self._name.ljust(9)} | enter client_id: ').strip()
        client_secret = getpass.getpass(f'{self._name.ljust(9)} | enter client_secret: ').strip()

        # write config
        self.log('encrypting reddit config')
        with open(self.reddit_config, 'w') as f:
            config = {
                'client_id': client_id,
                'client_secret': base64.b64encode(simplecrypt.encrypt(self.reddit_config, client_secret)).decode('utf-8')
            }
            json.dump(config, f, indent=4, sort_keys=True)

    def run(self):
        self._runevent.set()

        folder = os.path.join(self.root, 'data', self.subreddit)
        os.makedirs(folder, exist_ok=True)

        # download reddit data
        while not self.stopped():
            metadata = {os.path.basename(x): pd.read_csv(x, delimiter=';') for x in gb.glob(os.path.join(folder, '*.csv'))}
            for file_type, file_path in self.data.items():
                self.download(metadata, file_type, os.path.join(folder, file_path))
            self.log(f'sleep for {self.periode} seconds')
            self._time.sleep(self.periode)

        self._runevent.clear()

    def download(self, metadata, file_type, file_path):
        columns = [
            'id', 'author',
            'created', 'retrieved', 'edited',
            'gilded', 'pinned', 'archived', 'locked',
            'removed', 'deleted',
            'is_self', 'is_video', 'is_original_content',
            'title', 'link_flair_text', 'upvote_ratio', 'score',
            'total_awards_received', 'num_comments', 'num_crossposts',
            'selftext', 'thumbnail', 'shortlink'
        ]

        # import data
        df = pd.DataFrame(columns=columns).set_index('id')
        if os.path.exists(file_path):
            df = pd.read_hdf(file_path)

        # load metadata
        for file_path_metadata in metadata:
            df_metadata = metadata[file_path_metadata].sort_values(by=['created', 'retrieved'])
            df_metadata = df_metadata.drop_duplicates(file_type, keep='last').set_index(file_type)

            # validate metadata
            if df_metadata.empty:
                return

            # update last 8 hours
            df_metadata_exists = df_metadata[df_metadata.index.isin(df.index)]
            last_time = df_metadata_exists.iloc[-1]['created'] if not df_metadata_exists.empty else df_metadata.iloc[0]['created']
            update_time = last_time - (60 * 60 * 0)  # TODO 8h
            df_metadata_update = df_metadata[df_metadata['created'] >= update_time]

            self.log(f'update data after {datetime.fromtimestamp(update_time)} from {file_path_metadata}')

            # process submissions
            if file_type == 'submission':
                ids = list('t3_' + df_metadata_update.index)

                # fetch and update submission data
                data = self.fetch(file_type, ids)
                df_update = pd.DataFrame(data=data, columns=columns).set_index('id')
                df = df.combine_first(df_update)
                df.update(df_update)

                # updated data
                self.log(f'updated {df_update.shape[0]} {file_type}s')

        # convert float to int (pandas/issues/7509)
        num_columns = [x for x in df.select_dtypes(include='float64').columns if x not in ['upvote_ratio']]
        df[num_columns] = df[num_columns].apply(np.int64)

        # export data
        df = df.sort_values(by=['created', 'retrieved'])
        df.to_hdf(file_path, key='df', mode='w', complevel=9)
        df.to_html(f'{file_path}.html', max_rows=100, notebook=True, show_dimensions=True)

        # exported data
        self.log(f'exported {df.shape[0]} {file_type}s')

    def fetch(self, file_type, ids):
        data = []

        # chunk ids into lists with size 100
        self.log(f'download {len(ids)} {file_type}s')
        for fullnames in tqdm([ids[i:i + 100] for i in range(0, len(ids), 100)], desc=f'{self._name.ljust(9)} | fetching', unit_scale=100):
            now = datetime.now(timezone.utc).timestamp()

            # process submissions
            if file_type == 'submission':
                submissions = self.client.info(fullnames=fullnames)

                # submission data
                data = data + [[
                    str(s.id), str(s.author if s.author else '[deleted]'),
                    datetime.utcfromtimestamp(int(s.created_utc)), datetime.utcfromtimestamp(int(now)), datetime.utcfromtimestamp(int(s.edited)),
                    int(s.gilded), int(s.pinned), int(s.archived), int(s.locked),
                    int(s.selftext == '[removed]' or s.removed_by_category != None), int(s.selftext == '[deleted]'),
                    int(s.is_self), int(s.is_video), int(s.is_original_content),
                    str(s.title), str(s.link_flair_text), float(s.upvote_ratio), int(s.score),
                    int(s.total_awards_received), int(s.num_comments), int(s.num_crossposts),
                    str(s.selftext), str(s.thumbnail), str(s.shortlink)
                ] for s in submissions]

        return data

    def stop(self, timeout=None):
        self._stopevent.set()
        self._time.wake()

        while self.running():
            Sleep(0.1)

        if self.isAlive():
            self.join(timeout)


if __name__ == '__main__':
    reddit = Reddit(root=root, global_config=os.path.join('config', 'config.json'), reddit_config=os.path.join('config', '.reddit.json'))
    reddit.start()
