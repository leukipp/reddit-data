import requests

import pandas as pd

from datetime import datetime, timezone

from helper.env import Env
from helper.sleep import Sleep
from common.loader import Loader


class Pushshift(Loader):
    def __init__(self, root, config, subreddit):
        Loader.__init__(self, 'pushshift', root, config, subreddit)

        self.endpoint = 'https://api.pushshift.io/reddit/{}/search?limit=100&sort=created_utc&subreddit={}&after={}&before={}'

        # config parameters
        self.types = self.config['pushshift']['types']
        self.snapshot = self.config['pushshift']['snapshot']
        self.idle_periode = self.config['pushshift']['idle_periode']

        # initial run variables
        self.last_run = {}
        self.end_run = {}
        for file_type in self.types:
            self.last_run[file_type] = int(datetime.now(timezone.utc).timestamp())
            self.end_run[file_type] = self.config['pushshift']['start_time']

        # saved run variables
        for file_type in self.types:
            meta = self.read_meta(file_type)
            if 'last_run' in meta:
                self.last_run[file_type] = meta['last_run']
            if 'end_run' in meta:
                self.end_run[file_type] = meta['end_run']

    def run(self):
        self.runevent.set()

        try:
            # download pushshift metadata
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
        now = int(datetime.now(timezone.utc).timestamp())

        # set last run from now
        if self.last_run[file_type] == self.end_run[file_type]:
            self.last_run[file_type] = now

        self.log(f'download {file_type}s before {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")}')

        # define columns
        columns = {
            'submission': ['submission', 'subreddit', 'author', 'created', 'retrieved'],
            'comment': ['submission', 'comment', 'subreddit', 'author', 'created', 'retrieved']  # TODO fetch comments
        }[file_type]

        count = 0
        abort = False
        while not abort:

            # fetch data
            url = self.endpoint.format(file_type, self.subreddit, str(self.end_run[file_type]), str(self.last_run[file_type]))
            data = self.fetch(url, file_type)

            # validate data
            if data is None:
                if count == 0:
                    self.log(f'exported 0 {file_type}s')
                break

            # check errors
            abort = self.errors >= 12

            # build dataframe and sort
            df = pd.DataFrame(data, columns=columns).set_index(file_type)
            df = df.sort_values(by=['created', 'retrieved'])

            # check result
            if not df.empty:
                count += df.shape[0]

                # append data
                self.write_data(file_type, df, overwrite=False, snapshot=self.snapshot, last_run=self.last_run[file_type], end_run=self.end_run[file_type])
                self.log(f'exported {df.shape[0]} {file_type}s')

            # wait for next request
            Sleep(0.35)

        # abort on errors
        if abort:
            return

        # set last run and end run from now
        self.last_run[file_type] = now
        if count > 0:
            self.end_run[file_type] = now

        # update state
        self.write_meta(file_type, last_run=self.last_run[file_type], end_run=self.end_run[file_type])

    def fetch(self, url, file_type):
        try:
            # request data
            result = requests.get(url, headers={'User-Agent': Env.USER_AGENT()}).json()

            # validate result
            if 'data' not in result or not len(result['data']):
                self.log(f'fetched 0 {file_type}s after {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")}')
                return None

            # build data
            data = []
            for x in result['data']:

                # set last run from current item
                self.last_run[file_type] = x['created_utc'] - 1

                if file_type == 'submission' and 'selftext' in x and x['subreddit_type'] != 'user':
                    # parse submissions
                    data += [[
                        x['id'],
                        self.subreddit,
                        x['author'],
                        x['created_utc'],
                        x['retrieved_utc']
                    ]]
                elif file_type == 'comment' and 'body' in x:
                    # parse comments
                    data += [[
                        x['parent_id'].partition('_')[2],
                        x['id'],
                        self.subreddit,
                        x['author'],
                        x['created_utc'],
                        x['retrieved_utc']
                    ]]

            # fetched data
            self.log(f'fetched {len(data)} {file_type}s after {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")}')
            return data

        except Exception as e:
            self.log(f'...request error {repr(e)}, retry')
            self.errors += 1
            Sleep(10)

        return []
