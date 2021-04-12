import os
import sys
import csv
import json
import requests
import argparse

from lxml import html
from datetime import datetime, timezone

root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # nopep8
sys.path.insert(0, root)                                               # nopep8

from common.env import Env
from common.sleep import Sleep
from common.loader import Loader


class Crawler(Loader):
    def __init__(self, root, global_config, crawler_config):
        Loader.__init__(self, name='Crawler')

        self.root = root
        self.global_config = os.path.join(self.root, global_config)
        self.crawler_config = os.path.join(self.root, crawler_config)

        # load global config
        with open(self.global_config) as f:
            config = json.load(f)
            self.subreddit = config['subreddit']
            self.data = config['crawler']['data']
            self.periode = config['crawler']['periode']

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
        os.makedirs(os.path.dirname(self.crawler_config), exist_ok=True)
        with open(self.crawler_config, 'w') as f:
            config = {
                'last_run': self.last_run
            }
            json.dump(config, f, indent=4, sort_keys=True)

    def run(self):
        self._runevent.set()

        folder = os.path.join(self.root, 'data', 'private', self.subreddit)
        os.makedirs(folder, exist_ok=True)

        # download crawler metadata
        while not self.stopped():
            for file_type, file_path in self.data.items():
                self.download(file_type, os.path.join(folder, file_path))

            # periodic run
            if self.background():
                self.log(f'sleep for {self.periode} seconds')
                self._time.sleep(self.periode)
            else:
                break

        self._runevent.clear()

    def download(self, file_type, file_path):
        exists = os.path.exists(file_path)

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
            response = requests.get(url, headers={'User-Agent': Env.USER_AGENT()}).content
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
    argp = argparse.ArgumentParser()
    argp.add_argument('--config', type=str, required=True, help='file path of global config file')
    argp.add_argument('--background', action='store_true', help='run loaders periodic in background')
    args = argp.parse_args()

    # load config
    with open(os.path.join(root, args.config)) as f:
        config = json.load(f)

    # start crawler
    crawler = Crawler(root=root, global_config=args.config, crawler_config=os.path.join('config', config['subreddit'], 'crawler.json'))
    if args.background:
        crawler.start()
    else:
        crawler.run()
