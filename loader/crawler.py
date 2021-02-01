import os
import csv
import json
import time
import requests

from lxml import html
from datetime import datetime


class Crawler(object):
    def __init__(self, global_config, crawler_config):
        self.global_config = global_config
        self.crawler_config = crawler_config

        now = int(datetime.utcnow().timestamp())

        # load global config
        with open(global_config) as f:
            config = json.load(f)
            self.subreddit = config['subreddit']
            self.data = config['crawler']['data']

            self.last_run = {}
            for file_type in self.data.keys():
                self.last_run[file_type] = config['crawler']['end_run']

        # load private config
        config = self.read_config()
        if 'last_run' in config:
            for file_type in self.data.keys():
                self.last_run[file_type] = config['last_run'][file_type]
        self.url = 'https://old.reddit.com/r/{}/new/'

    def read_config(self):
        try:
            # read config
            print('\nloading crawler config')
            with open(self.crawler_config) as f:
                return json.load(f)
        except:
            return {}

    def write_config(self):
        # write config
        with open(self.crawler_config, 'w') as f:
            config = {
                'last_run': self.last_run
            }
            json.dump(config, f, indent=4, sort_keys=True)

    def run(self):
        folder = os.path.join('data', self.subreddit)
        os.makedirs(folder, exist_ok=True)

        # download crawler metadata
        for file_type, file_path in self.data.items():
            self.download(file_type, os.path.join(folder, file_path))

    def download(self, file_type, file_path):
        exists = os.path.exists(file_path)
        now = int(datetime.utcnow().timestamp())

        print(f'\ndownload {file_type}s after {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")} to {file_path}\n')
        with open(file_path, 'a+', newline='') as f:
            writer = csv.writer(f, delimiter=';')

            # write csv header
            if not exists:
                writer.writerow({
                    'submission': ['submission', 'author', 'created', 'retrieved'],
                    'comment': ['submission', 'comment', 'author', 'created', 'retrieved']
                }[file_type])

            # fetch data
            prefix = {
                'submission': 't3',
                'comment': 't1'
            }
            url = self.url.format(self.subreddit, prefix[file_type], self.last_run[file_type])
            data = self.fetch(url, file_type, [])

            # terminate
            if not len(data):
                return

            # update csv
            writer.writerows(data)

            # update last run
            created = [x[2] for x in data]
            self.last_run[file_type] = created[0]

            # update config
            self.write_config()

            # saved data
            print(f'saved {len(data)} {file_type}s')

    def fetch(self, url, file_type, data=[]):
        try:
            now = int(datetime.utcnow().timestamp())

            # terminate
            created = [x[2] for x in data]
            if len(created) and created[-1] <= self.last_run[file_type]:
                return [x for x in data if x[2] > self.last_run[file_type]]

            # request data
            response = requests.get(url, headers={'User-Agent': 'python:https://github.com/leukipp/TODO:v0.0.1 (by /u/leukipp)'}).content
            content = html.fromstring(response)

            # parse submissions
            things = content.xpath('.//div[contains(@class,"thing") and @data-fullname]')
            data = data + [[
                x.get('data-fullname').partition('_')[2].strip(),
                x.get('data-author'),
                int(int(x.get('data-timestamp')) / 1000),
                now
            ] for x in things if x.get('data-fullname').startswith('t3_')]

            # fetched data
            created = [x[2] for x in data]
            print(f'fetched {len(data)} {file_type}s after {datetime.fromtimestamp(created[-1]).strftime("%Y-%m-%d %H:%M:%S")}\n')

            # wait for next request
            time.sleep(0.35)

            # parse next url
            url_next = content.xpath('.//a[contains(@rel,"next") and @href]/@href')
            if len(url_next):
                return self.fetch(url_next[0], file_type, data)
        except Exception as e:
            print(f'...request error {repr(e)}, retry')
            time.sleep(1)

        return [x for x in data if x[2] > self.last_run[file_type]]