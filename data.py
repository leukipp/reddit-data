import os
import json
import argparse

import glob as gb

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
        with open(os.path.join(root, args.config)) as f:
            config = json.load(f)

        # pushshift
        pushshift = Pushshift(root=root, global_config=args.config, pushshift_config=os.path.join('config', config['subreddit'], 'pushshift.json'))
        threads.append(pushshift)

        # crawler
        crawler = Crawler(root=root, global_config=args.config, crawler_config=os.path.join('config', config['subreddit'], 'crawler.json'))
        threads.append(crawler)

        # reddit
        reddit = Reddit(root=root, global_config=args.config, reddit_config=os.path.join('config', config['subreddit'], 'reddit.json'))
        threads.append(reddit)

        # start threads
        for thread in threads:
            if args.background:
                thread.start()
            else:
                thread.run()

        # wait for abort
        while args.background:
            Sleep(1)
    except KeyboardInterrupt:
        # stop threads
        for thread in threads:
            thread.stop(1)


if __name__ == '__main__':
    argp = argparse.ArgumentParser()
    argp.add_argument('--config', type=str, required=False, help='file path of global config file')
    argp.add_argument('--background', action='store_true', help='run loaders periodic in background')
    args = argp.parse_args()

    try:
        if args.config:
            # single run
            main(args)
        else:
            # multi run
            while not args.background:
                for config in sorted(gb.glob(os.path.join('config', '*.json'))):
                    args.config = config
                    main(args)
    except KeyboardInterrupt as e:
        print(f'...aborted')
    except Exception as e:
        print(f'...error {repr(e)}')
