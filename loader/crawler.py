import os
import sys
import json
import requests
import argparse

import pandas as pd

from lxml import html
from datetime import datetime, timezone

root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # nopep8
sys.path.insert(0, root)                                               # nopep8

from helper.env import Env
from helper.sleep import Sleep
from common.loader import Loader


class Crawler(Loader):
    def __init__(self, root, config, subreddit):
        Loader.__init__(self, 'crawler', root, config, subreddit)

        self.endpoint = 'https://old.reddit.com/r/{}/new/'

        # config parameters
        self.types = self.config['crawler']['types']
        self.snapshot = self.config['crawler']['snapshot']
        self.idle_periode = self.config['crawler']['idle_periode']

        # initial run variables
        self.last_run = {}
        for file_type in self.types:
            self.last_run[file_type] = self.config['crawler']['start_time']

        # saved run variables
        for file_type in self.types:
            meta = self.read_meta(file_type)
            if 'last_run' in meta:
                self.last_run[file_type] = meta['last_run']

    def run(self):
        self.runevent.set()

        try:
            # download crawler metadata
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
        self.log(f'download {file_type}s after {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")}')

        # define columns
        columns = {
            'submission': ['submission', 'subreddit', 'author', 'created', 'retrieved'],
            'comment': ['submission', 'comment', 'subreddit', 'author', 'created', 'retrieved']  # TODO fetch comments
        }[file_type]

        # fetch data
        url = self.endpoint.format(self.subreddit)
        data = self.fetch(url, file_type, [])

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

    def fetch(self, url, file_type, data=[]):
        try:
            now = datetime.now(timezone.utc).timestamp()

            # terminate
            created = [x[3] for x in data]
            if len(created) and created[-1] <= self.last_run[file_type]:
                return [x for x in data if x[3] > self.last_run[file_type]]

            # request data
            response = requests.get(url, headers={'User-Agent': Env.USER_AGENT()}).content
            content = html.fromstring(response)

            # parse submissions
            things = content.xpath('.//div[contains(@class,"thing") and @data-fullname]')
            data += [[
                x.get('data-fullname').partition('_')[2].strip(),
                self.subreddit,
                x.get('data-author'),
                int(x.get('data-timestamp')) // 1000,
                int(now)
            ] for x in things if x.get('data-fullname').startswith('t3_')]

            # fetched data
            created = [x[3] for x in data]
            self.log(f'fetched {len(data)} {file_type}s after {datetime.fromtimestamp(created[-1]).strftime("%Y-%m-%d %H:%M:%S")}')

            # wait for next request
            Sleep(0.35)

            # parse next url
            url_next = content.xpath('.//a[contains(@rel,"next") and @href]/@href')
            if len(url_next):
                return self.fetch(url_next[0], file_type, data)

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

    # start crawler
    crawler = Crawler(root, config, args.subreddit)
    if args.background:
        crawler.start()
    else:
        crawler.run()
