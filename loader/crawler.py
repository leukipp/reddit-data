import os
import csv
import json
import requests

from lxml import html
from datetime import datetime, timezone

from common.sleep import Sleep
from common.loader import Loader


class Crawler(Loader):
    def __init__(self, global_config, crawler_config):
        Loader.__init__(self, name='Crawler')

        self.run_periode = 10
        self.global_config = global_config
        self.crawler_config = crawler_config

        now = int(datetime.now(timezone.utc).timestamp())

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
            self.log('loading crawler config')
            with open(self.crawler_config) as f:
                return json.load(f)
        except KeyboardInterrupt:
            raise KeyboardInterrupt
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
        self._runevent.set()

        folder = os.path.join('data', self.subreddit)
        os.makedirs(folder, exist_ok=True)

        # download crawler metadata
        while not self.stopped():
            for file_type, file_path in self.data.items():
                self.download(file_type, os.path.join(folder, file_path))
            self._time.sleep(self.run_periode)

        self._runevent.clear()

    def download(self, file_type, file_path):
        exists = os.path.exists(file_path)
        now = int(datetime.now(timezone.utc).timestamp())

        self.log(f'download {file_type}s after {datetime.fromtimestamp(self.last_run[file_type]).strftime("%Y-%m-%d %H:%M:%S")} to {file_path}')
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
            self.log(f'saved {len(data)} {file_type}s')

    def fetch(self, url, file_type, data=[]):
        try:
            now = int(datetime.now(timezone.utc).timestamp())

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
            self.log(f'fetched {len(data)} {file_type}s after {datetime.fromtimestamp(created[-1]).strftime("%Y-%m-%d %H:%M:%S")}')

            # wait for next request
            Sleep(0.35)

            # parse next url
            url_next = content.xpath('.//a[contains(@rel,"next") and @href]/@href')
            if len(url_next):
                return self.fetch(url_next[0], file_type, data)
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            self.log(f'...request error {repr(e)}, retry')
            Sleep(1)

        return [x for x in data if x[2] > self.last_run[file_type]]

    def stop(self, timeout=None):
        self._stopevent.set()
        self._time.wake()

        while self.running():
            Sleep(0.1)

        if self.isAlive():
            self.join(timeout)


if __name__ == '__main__':
    crawler = Crawler(global_config=os.path.join('config', 'config.json'), crawler_config=os.path.join('config', '.crawler.json'))
    crawler.start()
