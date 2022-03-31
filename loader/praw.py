import os
import sys
import csv
import json
import argparse

import pandas as pd

from tqdm import tqdm
from praw import Reddit
from datetime import datetime, timezone

root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # nopep8
sys.path.insert(0, root)                                               # nopep8

from helper.env import Env
from helper.sleep import Sleep
from common.store import Store
from common.loader import Loader


class Praw(Loader):
    def __init__(self, root, config, subreddit):
        Loader.__init__(self, 'praw', root, config, subreddit)

        self.endpoint = {
            'user_agent': Env.USER_AGENT(),
            'client_id': Env.REDDIT_CLIENT_ID(),
            'client_secret': Env.REDDIT_CLIENT_SECRET()
        }
        self.reddit = Reddit(**self.endpoint)

        # config parameters
        self.types = self.config['praw']['types']
        self.periode = self.config['praw']['periode']
        self.retrospect_time = self.config['praw']['retrospect_time']

        # initial run variables
        self.last_run = {}
        for file_type in self.types:
            self.last_run[file_type] = 0

        # saved run variables
        for file_type in self.types:
            meta = self.read_meta(file_type)
            if 'last_run' in meta:
                self.last_run[file_type] = meta['last_run']

    def run(self):
        self.runevent.set()

        try:
            # download reddit data
            while not self.stopped():
                stores = [
                    Store('crawler', self.root, self.config, self.subreddit),
                    Store('pushshift', self.root, self.config, self.subreddit)
                ]
                for file_type in self.types:
                    self.download(file_type, stores)

                # periodic run
                if self.alive():
                    self.log(f'sleep for {self.periode} seconds')
                    self.time.sleep(self.periode)
                else:
                    break

        except KeyboardInterrupt:
            self.runevent.clear()
            raise KeyboardInterrupt()
        except Exception as e:
            self.log(f'...run error {repr(e)}')

        self.runevent.clear()

    def download(self, file_type, stores):
        now = int(datetime.now(timezone.utc).timestamp())

        # set last run from now
        self.last_run[file_type] = now

        # define columns
        columns = {
            'submission': [
                'submission', 'subreddit', 'author',
                'created', 'retrieved', 'edited',
                'pinned', 'archived', 'locked',
                'removed', 'deleted',
                'is_self', 'is_video', 'is_original_content',
                'title', 'link_flair_text', 'upvote_ratio', 'score',
                'gilded', 'total_awards_received', 'num_comments', 'num_crossposts',
                'selftext', 'thumbnail', 'shortlink'
            ],
            'comment': [
                'submission', 'subreddit', 'comment', 'author',
                'created', 'retrieved'  # TODO fetch comments
            ]
        }[file_type]

        # read existing data
        df = self.read_data(file_type)
        if df.empty:
            df = pd.DataFrame(columns=columns).set_index(file_type)
        idxs = list(df.index)

        # load metadata
        for store in stores:
            df_store = store.read_data(file_type)

            # validate dataset
            if df_store.empty:
                continue
            df_store_existing = df_store[df_store.index.isin(idxs)].sort_values(by=['created', 'retrieved'])

            # update last x hours based on retrospect time sliding window
            last_time = df_store.iloc[0]['created'] if df_store_existing.empty else df_store_existing.iloc[-1]['created']
            update_time = last_time - (60 * 60 * self.retrospect_time)

            self.log(f'update data after {datetime.fromtimestamp(update_time)} from {store.name}')

            # obtain fetch ids
            prefix = {
                'submission': 't3_',
                'comment': 't1_'
            }
            df_store_update = df_store[df_store['created'] >= update_time]
            ids = list(prefix[file_type] + df_store_update.index)

            # process submissions
            if file_type == 'submission':

                # fetch data
                data = self.fetch(file_type, ids)

                # update submission data
                df_update = pd.DataFrame(data, columns=columns).set_index(file_type)
                df = df.combine_first(df_update)
                df.update(df_update)

                # updated data
                self.log(f'updated {df_update.shape[0]} {file_type}s')

        # convert datatypes
        df = df.convert_dtypes().sort_values(by=['created', 'retrieved'])

        # write data
        self.write_data(file_type, df, overwrite=True, last_run=self.last_run[file_type])
        self.log(f'exported {df.shape[0]} {file_type}s')

        # export data
        file_path = os.path.join(self.root, 'data', 'export', self.subreddit, f'{file_type}.csv')
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, header=True, index=True, doublequote=True, quoting=csv.QUOTE_NONNUMERIC, sep=',', encoding='utf-8')

    def fetch(self, file_type, ids):
        try:
            data = []

            # chunk id's into batches of size 100
            self.log(f'download {len(ids)} {file_type}s')
            batches = [ids[i:i + 100] for i in range(0, len(ids), 100)]
            for fullnames in tqdm(batches, desc=self.text() + 'fetching', unit_scale=100):
                now = datetime.now(timezone.utc).timestamp()

                # process submissions
                if file_type == 'submission':

                    # request data
                    submissions = self.reddit.info(fullnames=fullnames)

                    # parse submissions
                    data += [[
                        str(x.id), str(self.subreddit), str(x.author.name if x.author else '[deleted]'),
                        int(x.created_utc), int(now), int(x.edited),
                        int(x.pinned), int(x.archived), int(x.locked),
                        int(x.selftext == '[removed]' or x.removed_by_category != None), int(x.selftext == '[deleted]'),
                        int(x.is_self), int(x.is_video), int(x.is_original_content),
                        str(x.title), str(x.link_flair_text), float(x.upvote_ratio), int(x.score),
                        int(x.gilded), int(x.total_awards_received), int(x.num_comments), int(x.num_crossposts),
                        str(x.selftext), str(x.thumbnail), str(x.shortlink)
                    ] for x in submissions]

                # wait for next request
                Sleep(0.35)

            return data

        except Exception as e:
            self.log(f'...request error {repr(e)}, retry')
            Sleep(1)

        return []


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('subreddit', type=str, help='subreddit to fetch data from')
    argp.add_argument('-config', type=str, default=os.path.join('config', 'loader.json'), help='file path of global config file')
    argp.add_argument('-background', action='store_true', default=False, help='run loaders periodically in background')
    args = argp.parse_args()

    # load config
    with open(os.path.join(root, args.config)) as f:
        config = json.load(f)

    # start praw
    praw = Praw(root, config, args.subreddit)
    if args.background:
        praw.start()
    else:
        praw.run()
