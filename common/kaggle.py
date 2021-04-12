import os
import json
import tempfile

import glob as gb
import pandas as pd

from common.env import Env

from datetime import datetime, timezone
from kaggle.api.kaggle_api_extended import KaggleApi


class Kaggle(object):
    def __init__(self):
        self.kaggle = KaggleApi()
        self.kaggle.authenticate()

        self.descriptions = {
            'id': 'The id of the submission.',
            'author': 'The redditors username.',
            'created': 'Time the submission was created.',
            'retrieved': 'Time the submission was retrieved.',
            'edited': 'Time the submission was modified.',
            'pinned': 'Whether or not the submission is pinned.',
            'archived': 'Whether or not the submission is archived.',
            'locked': 'Whether or not the submission is locked.',
            'removed': 'Whether or not the submission is mod removed.',
            'deleted': 'Whether or not the submission is user deleted.',
            'is_self': 'Whether or not the submission is a text.',
            'is_video': 'Whether or not the submission is a video.',
            'is_original_content': 'Whether or not the submission has been set as original content.',
            'title': 'The title of the submission.',
            'link_flair_text': 'The submission link flairs text content.',
            'upvote_ratio': 'The percentage of upvotes from all votes on the submission.',
            'score': 'The number of upvotes for the submission.',
            'gilded': 'The number of gilded awards on the submission.',
            'total_awards_received': 'The number of awards on the submission.',
            'num_comments': 'The number of comments on the submission.',
            'num_crossposts': 'The number of crossposts on the submission.',
            'selftext': 'The submission selftext on text posts.',
            'thumbnail': 'The submission thumbnail on image posts.',
            'shortlink': 'The submission short url.'
        }

        self.types = {
            'object': 'string',
            'int64': 'integer',
            'float64': 'number',
            'datetime64[ns]': 'datetime'
        }

    def _update(self, root):
        summary = {}
        resources = []

        for f in sorted(gb.glob(os.path.join(root, '**', '*.*'))):
            df = pd.read_hdf(f.replace('.csv', '.h5')).reset_index()
            name = os.path.basename(f)
            path = os.path.join(*(f.split(os.path.sep)[2:]))
            link = f'r/{os.path.dirname(path)}'
            description = f'[{link}](https://reddit.com/{link}): #{df.shape[0]} ({df["created"].min()} - {df["created"].max()})'

            fields = [{
                'name': f'{column}',
                'title': f'{column}',
                'description': self.descriptions[column],
                'type': self.types[df.dtypes.astype(str)[column]]
            } for column in df.columns]

            resources.append({
                'name': name,
                'path': path,
                'description': description,
                'schema': {
                    'fields': fields
                }
            })

            summary[description] = df.shape[0]

        # read markdown
        md = open(os.path.join(root.replace('public', 'private'), 'datapackage.md')).read()
        md_description = [f'- {x[0]}' for x in sorted(summary.items(), key=lambda x: x[1], reverse=True)]
        md_data = [f'- `{x["name"]}` (*{x["type"]}*): {x["description"]}' for x in fields]
        md_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # read metadata
        metadata_path = os.path.join(root, 'datapackage.json')
        with open(metadata_path) as f:
            metadata = json.load(f)

        # update metadata
        metadata['description'] = md.format('\n'.join(md_description), '\n'.join(md_data), md_date)
        metadata['resources'] = resources
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)

        # return update message
        return 'update data'

    def download(self, dataset, local=False):
        workspace = Env.VSCODE_WORKSPACE()
        if local and workspace:
            return os.path.join(workspace, 'data', 'public')
        path = tempfile.mkdtemp()
        self.kaggle.dataset_download_files(dataset, path=path, quiet=False, force=True, unzip=True)
        return path

    def upload(self, path):
        self._update(root=path)
        return self.kaggle.dataset_create_version(path, version_notes=self._update(root=path), dir_mode='zip')
