#!/usr/bin/env python3

import os
import json
import signal
import argparse

from helper.env import Env
from helper.sleep import Sleep
from common.kaggle import Kaggle
from common.logger import Logger

from loader.praw import Praw
from loader.search import Search
from loader.crawler import Crawler
from loader.pushshift import Pushshift


terminated = False


def fetch(root, config, subreddit):
    logger = Logger('main', 'fetch', plain=True)
    loaders = []

    try:

        # search
        if 'search' in config:
            search = Search(root, config, subreddit)
            loaders.append(search)

        # crawler
        if 'crawler' in config:
            crawler = Crawler(root, config, subreddit)
            loaders.append(crawler)

        # pushshift
        if 'pushshift' in config:
            pushshift = Pushshift(root, config, subreddit)
            loaders.append(pushshift)

        # praw
        if 'praw' in config:
            praw = Praw(root, config, subreddit)
            loaders.append(praw)

        # start loader threads
        background = False  # TODO thread implementation
        for loader in loaders:
            if background:
                loader.start()
            else:
                loader.run()

        # wait until abort
        while background:
            Sleep(1)

    except KeyboardInterrupt:
        for loader in loaders:
            loader.stop(1)
        raise KeyboardInterrupt()

    except Exception as e:
        logger.log(f'...fetch error {repr(e)}')


def publish(interval, kaggle):
    logger = Logger('main', 'publish', plain=True)

    try:

        # upload disabled
        if not interval:
            return

        # update datapackage
        kaggle.update()

        # start upload
        elapsed = kaggle.timer.stop(run=False) / 1000
        if elapsed > interval:
            logger.log(f'\n{"-"*45}{"UPLOADING":^15}{"-"*45}\n')
            kaggle.upload()
            logger.log(f'\n{"-"*45}{"PUBLISHED":^15}{"-"*45}\n')
            kaggle.timer.reset()

    except Exception as e:
        logger.log(f'...publish error {repr(e)}')


def terminate(sig, frm):
    logger = Logger('main', 'terminate', plain=True)
    logger.log(f'\n{"-"*45}{"TERMINATED":^15}{"-"*45}\n')

    # global termination
    global terminated
    terminated = True


def main(args):
    logger = Logger('main', 'main', plain=True)
    logger.log(f'\n{"-"*45}{"ENVIRONMENT":^15}{"-"*45}\n\n{Env()}')
    logger.log(f'\n{"-"*45}{"STARTED":^15}{"-"*45}\n')

    try:

        # load config
        root = os.path.abspath(os.path.dirname(__file__))
        with open(os.path.join(root, args.config)) as f:
            config = json.load(f)

        # kaggle client
        kaggle_path = os.path.join('data', 'export')
        kaggle_config = os.path.join('config', 'kaggle.json')
        kaggle = Kaggle(kaggle_config, kaggle_path)

        # start background tasks
        while len(args.subreddits) and not terminated:
            for subreddit in args.subreddits:

                # fetch data
                fetch(root, config, subreddit)

                # pause requests
                if args.pause:
                    logger.log(f'\n{"-"*45}{"PAUSING":^15}{"-"*45}\n')
                    Sleep(args.pause)
                else:
                    logger.log(f'\n{"-"*105}\n')

                # check termination
                if terminated:
                    break
            else:
                # publish data
                publish(args.publish, kaggle)

    except KeyboardInterrupt as e:
        logger.log(f'\n...aborted')

    except Exception as e:
        logger.log(f'...error {repr(e)}')

    finally:
        logger.log(f'\n{"-"*45}{"STOPPED":^15}{"-"*45}\n')


if __name__ == '__main__':

    # parse console arguments
    argp = argparse.ArgumentParser()
    if not Env.SUBREDDITS():
        argp.add_argument('subreddits', type=str, nargs='+', help='subreddits to fetch data from')
    else:
        argp.add_argument('-subreddits', type=str, nargs='+', default=Env.SUBREDDITS().split(), help='subreddits to fetch data from')
    argp.add_argument('-config', type=str, default=os.path.join('config', 'loader.json'), help='file path of global config file')
    argp.add_argument('-background', action='store_true', default=False, help='run loaders periodically in background')
    argp.add_argument('-publish', type=int, default=Env.PUBLISH(), help='publish datasets to kaggle every x seconds')
    argp.add_argument('-pause', type=int, default=Env.PAUSE(), help='pause x seconds after fetching a subreddit')
    args = argp.parse_args()

    # handle process termination
    signal.signal(signal.SIGTERM, terminate)

    # start main procedure
    main(args)
