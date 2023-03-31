import os
import sys
import csv
import json
import argparse

import pandas as pd

from praw import Reddit
from praw.models import Subreddit
from datetime import datetime, timezone

root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # nopep8
sys.path.insert(0, root)                                               # nopep8

from helper.env import Env
from helper.sleep import Sleep
from common.loader import Loader


class Search(Loader):
    def __init__(self, root, config, subreddit):
        Loader.__init__(self, 'search', root, config, subreddit)

        self.endpoint = {
            'user_agent': Env.USER_AGENT(),
            'client_id': Env.REDDIT_CLIENT_ID(),
            'client_secret': Env.REDDIT_CLIENT_SECRET()
        }
        self.reddit = Reddit(**self.endpoint)

        # config parameters
        self.types = self.config['search']['types']
        self.snapshot = self.config['search']['snapshot']
        self.idle_periode = self.config['search']['idle_periode']

        # initial run variables
        self.last_run = {}
        for file_type in self.types:
            self.last_run[file_type] = self.config['search']['start_time']

        # saved run variables
        for file_type in self.types:
            meta = self.read_meta(file_type)
            if 'last_run' in meta:
                self.last_run[file_type] = meta['last_run']

    def run(self):
        self.runevent.set()

        try:
            # download search metadata
            while not self.stopped():
                for file_type in self.types:
                    self.download(file_type)

                # periodic run
                if self.alive():
                    self.log(f'sleep for {self.idle_periode} seconds')
                    self.time.sleep(self.idle_periode)
                else:
                    break

        except KeyboardInterrupt:
            self.runevent.clear()
            raise KeyboardInterrupt()
        except Exception as e:
            self.log(f'...run error {repr(e)}')

        self.runevent.clear()

    def download(self, file_type):
        self.log(f'download {file_type}s before {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")}')

        # define columns
        columns = {
            'submission': ['submission', 'subreddit', 'author', 'created', 'retrieved'],
            'comment': ['submission', 'comment', 'subreddit', 'author', 'created', 'retrieved']  # TODO fetch comments
        }[file_type]

        # fetch data
        query = f'r/{self.subreddit}'
        data = self.fetch(query, file_type, [])

        # build dataframe and sort
        df = pd.DataFrame(data, columns=columns).set_index(file_type)
        df = df.sort_values(by=['created', 'retrieved'])

        # validate data
        if df.empty:
            self.log(f'exported 0 {file_type}s')
            return

        # set last run from last item
        self.last_run[file_type] = int(df.iloc[-1]['created'])

        # append data
        self.write_data(file_type, df, overwrite=False, snapshot=self.snapshot, last_run=self.last_run[file_type])
        self.log(f'exported {df.shape[0]} {file_type}s')

    def fetch(self, query, file_type, data=[]):
        try:
            now = datetime.now(timezone.utc).timestamp()

            # process submissions
            if file_type == 'submission':

                # search over all subs
                all = Subreddit(self.reddit, 'all')
                submissions = all.search(query, sort='new')

                # parse submissions
                data += [[
                    str(x.id),
                    str(x.subreddit).lower(),
                    str(x.author.name if x.author else '[deleted]'),
                    int(x.created_utc),
                    int(now)
                ] for x in submissions]

            # fetched data
            created = [x[3] for x in data]
            self.log(f'fetched {len(data)} {file_type}s after {datetime.fromtimestamp(created[-1]).strftime("%Y-%m-%d %H:%M:%S")}')

        except Exception as e:
            self.log(f'...request error {repr(e)}, retry')
            Sleep(10)

        return [x for x in data if x[3] > self.last_run[file_type]]


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('subreddit', type=str, help='subreddit to fetch data from')
    argp.add_argument('-config', type=str, default=os.path.join('config', 'loader.json'), help='file path of global config file')
    argp.add_argument('-background', action='store_true', default=False, help='run loaders periodically in background')
    args = argp.parse_args()

    # load config
    with open(os.path.join(root, args.config)) as f:
        config = json.load(f)

    # start search
    search = Search(root, config, args.subreddit)
    if args.background:
        search.start()
    else:
        search.run()
