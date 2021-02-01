import os
import json
import argparse

from loader.reddit import Reddit
from loader.pushshift import Pushshift


def main():
    argp = argparse.ArgumentParser(description='wsbyolo')
    argp.add_argument('--global-config', default=os.path.join('config', 'config.json'), type=str, help='file path of global config file [PATH]')
    argp.add_argument('--pushshift-config', default=os.path.join('config', '.pushshift.json'), type=str, help='file path of private pushshift config file [PATH]')
    argp.add_argument('--reddit-config', default=os.path.join('config', '.reddit.json'), type=str, help='file path of private reddit config file [PATH]')
    args = argp.parse_args()

    # pushshift
    pushshift = Pushshift(global_config=args.global_config, pushshift_config=args.pushshift_config)
    pushshift.run()

    # reddit
    reddit = Reddit(global_config=args.global_config, reddit_config=args.reddit_config)
    reddit.run()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f'\n...aborted')
    except Exception as e:
        print(f'...error {repr(e)}')