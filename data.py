import os
import json
import argparse

from common.sleep import Sleep
from common.prompt import Prompt

from loader.reddit import Reddit
from loader.crawler import Crawler
from loader.pushshift import Pushshift


def main(args):
    threads = []

    try:
        # load config
        root = os.path.abspath(os.path.dirname(__file__))
        with open(os.path.join(root, args.global_config)) as f:
            config = json.load(f)

        # pushshift
        pushshift = Pushshift(root=root, global_config=args.global_config, pushshift_config=os.path.join('config', config['subreddit'], 'pushshift.json'))
        threads.append(pushshift)

        # crawler
        crawler = Crawler(root=root, global_config=args.global_config, crawler_config=os.path.join('config', config['subreddit'], 'crawler.json'))
        threads.append(crawler)

        # reddit
        reddit = Reddit(root=root, global_config=args.global_config, reddit_config=os.path.join('config', config['subreddit'], 'reddit.json'))
        threads.append(reddit)

        # start threads
        for thread in threads:
            thread.start()

        # wait for abort
        while True:
            Sleep(1)
    except KeyboardInterrupt:
        # stop threads
        for thread in threads:
            thread.stop(1)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--global-config',  type=str, required=True, help='file path of global config file [PATH]')
    args = argp.parse_args()

    try:
        main(args)
    except KeyboardInterrupt as e:
        print(f'...aborted')
    except Exception as e:
        print(f'...error {repr(e)}')
