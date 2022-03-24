import os
import json
import signal
import numexpr
import argparse

from helper.env import Env
from helper.sleep import Sleep
from common.kaggle import Kaggle
from common.logger import Logger

from loader.praw import Praw
from loader.crawler import Crawler
from loader.pushshift import Pushshift


terminated = False


def terminate(sig, frm):
    global terminated
    terminated = True
    logger.log(f'\n{"-"*45}{"TERMINATED":^15}{"-"*45}\n')


def fetch(config, subreddit):
    logger = Logger('main', 'fetch', plain=True)

    loaders = []
    try:
        # pushshift
        pushshift = Pushshift(root, config, subreddit)
        loaders.append(pushshift)

        # crawler
        crawler = Crawler(root, config, subreddit)
        loaders.append(crawler)

        # praw
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

    path = os.path.join('data', 'export')
    try:
        # update datapackage
        kaggle.update(path)

        # upload disabled
        if not interval:
            return

        # start upload
        elapsed = kaggle.timer.stop(run=False) / 1000
        if elapsed > interval:
            logger.log(f'\n{"-"*45}{"UPLOADING":^15}{"-"*45}\n')
            kaggle.upload(path)
            logger.log(f'\n{"-"*45}{"PUBLISHED":^15}{"-"*45}\n')
            kaggle.timer.reset()

    except Exception as e:
        logger.log(f'...publish error {repr(e)}')


if __name__ == '__main__':
    logger = Logger('main', 'main', plain=True)

    # parse console arguments
    argp = argparse.ArgumentParser()
    argp.add_argument('subreddits', type=str, nargs='+', help='subreddits to fetch data from')
    argp.add_argument('-config', type=str, default=os.path.join('config', 'loader.json'), help='file path of global config file')
    argp.add_argument('-background', action='store_true', default=False, help='run loaders periodically in background')
    argp.add_argument('-publish', type=int, default=None, help='publish datasets to kaggle every x seconds')
    argp.add_argument('-pause', type=int, default=None, help='pause x seconds after fetching a subreddit')
    args = argp.parse_args()

    # handle process termination
    signal.signal(signal.SIGTERM, terminate)

    try:
        logger.log(f'\n{"-"*45}{"ENVIRONMENT":^15}{"-"*45}\n')
        logger.log(Env.init())
        logger.log(f'\n{"-"*45}{"STARTED":^15}{"-"*45}\n')

        # load config
        root = os.path.abspath(os.path.dirname(__file__))
        with open(os.path.join(root, args.config)) as f:
            config = json.load(f)

        # kaggle client
        kaggle = Kaggle(config=os.path.join('config', 'kaggle.json'))

        # start background tasks
        while not terminated:

            # fetch data
            for subreddit in args.subreddits:
                if terminated:
                    break
                fetch(config, subreddit)

                if args.pause:
                    logger.log(f'\n{"-"*45}{"PAUSING":^15}{"-"*45}\n')
                    Sleep(args.pause)
                else:
                    logger.log(f'\n{"-"*105}\n')

            # publish data
            if not terminated:
                publish(args.publish, kaggle)

    except KeyboardInterrupt as e:
        logger.log(f'...aborted')
    except Exception as e:
        logger.log(f'...error {repr(e)}')
    finally:
        logger.log(f'\n{"-"*45}{"STOPPED":^15}{"-"*45}\n')
