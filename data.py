import os
import argparse

from common.sleep import Sleep
from common.prompt import Prompt

from loader.reddit import Reddit
from loader.crawler import Crawler
from loader.pushshift import Pushshift


def main(args):
    threads = []

    try:
        # pushshift
        pushshift = Pushshift(global_config=args.global_config, pushshift_config=args.pushshift_config)
        threads.append(pushshift)

        # crawler
        crawler = Crawler(global_config=args.global_config, crawler_config=args.crawler_config)
        threads.append(crawler)

        # reddit
        reddit = Reddit(global_config=args.global_config, reddit_config=args.reddit_config)
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
    argp = argparse.ArgumentParser(description='wsbyolo')
    argp.add_argument('--global-config', default=os.path.join('config', 'config.json'), type=str, help='file path of global config file [PATH]')
    argp.add_argument('--pushshift-config', default=os.path.join('config', '.pushshift.json'), type=str, help='file path of private pushshift config file [PATH]')
    argp.add_argument('--crawler-config', default=os.path.join('config', '.crawler.json'), type=str, help='file path of private crawler config file [PATH]')
    argp.add_argument('--reddit-config', default=os.path.join('config', '.reddit.json'), type=str, help='file path of private reddit config file [PATH]')

    try:
        main(argp.parse_args())
    except KeyboardInterrupt as e:
        print(f'...aborted')
    except Exception as e:
        print(f'...error {repr(e)}')
