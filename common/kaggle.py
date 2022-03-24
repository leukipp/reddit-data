import os
import csv
import json
import tempfile

import glob as gb
import pandas as pd

from helper.env import Env
from helper.timer import Timer

from datetime import datetime, timezone
from kaggle.api.kaggle_api_extended import KaggleApi


class Kaggle(object):
    def __init__(self, config):
        self.config = config

        self.kaggle = KaggleApi()
        self.kaggle.authenticate()

        self.descriptions = {
            'submission': {
                'submission': 'The id of the submission',
                'subreddit': 'The subreddit name',
                'author': 'The redditors username',
                'created': 'Time the submission was created',
                'retrieved': 'Time the submission was retrieved',
                'edited': 'Time the submission was modified',
                'pinned': 'Whether or not the submission is pinned',
                'archived': 'Whether or not the submission is archived',
                'locked': 'Whether or not the submission is locked',
                'removed': 'Whether or not the submission is mod removed',
                'deleted': 'Whether or not the submission is user deleted',
                'is_self': 'Whether or not the submission is a text',
                'is_video': 'Whether or not the submission is a video',
                'is_original_content': 'Whether or not the submission has been set as original content',
                'title': 'The title of the submission',
                'link_flair_text': 'The submission link flairs text content',
                'upvote_ratio': 'The percentage of upvotes from all votes on the submission',
                'score': 'The number of upvotes for the submission',
                'gilded': 'The number of gilded awards on the submission',
                'total_awards_received': 'The number of awards on the submission',
                'num_comments': 'The number of comments on the submission',
                'num_crossposts': 'The number of crossposts on the submission',
                'selftext': 'The submission selftext on text posts',
                'thumbnail': 'The submission thumbnail on image posts',
                'shortlink': 'The submission short url'
            },
            'comment': {
                # TODO fetch comments
            }
        }

        self.datatypes = {
            'object': 'string',
            'int64': 'integer',
            'float64': 'number',
            'datetime64[ns]': 'datetime'
        }

        self.timer = Timer()

    def download(self, dataset, local=False):

        # use local folder
        if local and Env.VSCODE_WORKSPACE():
            return os.path.join(Env.VSCODE_WORKSPACE(), 'data', 'export')

        # download dataset
        path = tempfile.mkdtemp()
        self.kaggle.dataset_download_files(dataset, path=path, quiet=False, force=True, unzip=True)

        # return dataset path
        return path

    def update(self, root):
        summary = {}
        resources = []

        # read metadata from files
        for file_path in sorted(gb.glob(os.path.join(root, '**', '*.csv'))):
            df = pd.read_csv(file_path, doublequote=True, quoting=csv.QUOTE_NONNUMERIC, sep=',', encoding='utf-8')

            count = df.shape[0]
            name = os.path.basename(file_path)
            path = os.path.join(*(file_path.split(os.path.sep)[2:]))
            link = f'r/{os.path.dirname(path)}'

            # ignore empty files
            if count == 0:
                continue

            # build description
            time_from = datetime.fromtimestamp(df['created'].min()).strftime('%Y-%m-%d %H:%M:%S')
            time_to = datetime.fromtimestamp(df['created'].max()).strftime('%Y-%m-%d %H:%M:%S')
            description = f'[{link}](https://reddit.com/{link}) | {time_from} | {time_to} | *{df.shape[0]}*'

            # build ressources
            resources.append({
                'name': name,
                'path': path,
                'description': description,
                'schema': {
                    'fields': [{
                        'name': f'{column}',
                        'title': f'{column}',
                        'description': self.descriptions[name.split('.')[0]][column],
                        'type': self.datatypes[df.dtypes.astype(str)[column]]
                    } for column in df.columns]
                }
            })

            # save number of entries
            summary[description] = count

        # read kaggle template
        template = {}
        with open(self.config) as f:
            template = json.load(f)

        # build readme
        md = template['description']
        md_description = [f'{x[0]}' for x in sorted(summary.items(), key=lambda x: x[1], reverse=True)]
        md_data = [f'`{x["name"]}` | {x["description"]} | *{x["type"]}*' for x in resources[0]['schema']['fields']]
        md_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # export readme file
        readme = md.format('\n'.join(md_description), '\n'.join(md_data), md_date)
        with open(os.path.join(root, 'README.md'), 'w') as f:
            f.write(readme)

        # export datapackage file
        template['description'] = readme
        template['resources'] = resources
        with open(os.path.join(root, 'datapackage.json'), 'w') as f:
            json.dump(template, f, indent=4)

        # update message
        return f'{md_date} - {sum([x for x in summary.values()])}'

    def upload(self, path):
        return self.kaggle.dataset_create_version(path, version_notes=self.update(root=path), dir_mode='zip')
