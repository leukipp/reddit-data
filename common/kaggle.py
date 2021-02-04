import os
import json
import tempfile

import glob as gb
import pandas as pd

from datetime import datetime, timezone
from kaggle.api.kaggle_api_extended import KaggleApi


class Kaggle(object):
    def __init__(self):
        self.kaggle = KaggleApi()
        self.kaggle.authenticate()
        self.descriptions = {
            'author': 'desc',
            'created': 'desc',
            'retrieved': 'desc',
            'edited': 'desc',
            'pinned': 'desc',
            'archived': 'desc',
            'locked': 'desc',
            'removed': 'desc',
            'deleted': 'desc',
            'is_self': 'desc',
            'is_video': 'desc',
            'is_original_content': 'desc',
            'title': 'desc',
            'link_flair_text': 'desc',
            'upvote_ratio': 'desc',
            'score': 'desc',
            'gilded': 'desc',
            'total_awards_received': 'desc',
            'num_comments': 'desc',
            'num_crossposts': 'desc',
            'selftext': 'desc',
            'thumbnail': 'desc',
            'shortlink': 'desc'
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
            df = pd.read_hdf(f.replace('.csv', '.h5'))
            name = os.path.basename(f)
            path = os.path.join(*(f.split(os.path.sep)[2:]))
            link = f'r/{os.path.dirname(path)}'
            description = f'[{link}](https://reddit.com/{link}): #{df.shape[0]} ({df["created"].min()} - {df["created"].max()})'

            fields = [{
                'name': f'{column}',
                'title': f'title: {column}',
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

    def download(self, dataset):
        if 'VSCODE_WORKSPACE' in os.environ:
            return os.path.join(os.environ['VSCODE_WORKSPACE'], 'data', 'public')
        path = tempfile.mkdtemp()
        self.kaggle.dataset_download_files(dataset, path=path, quiet=False, force=True, unzip=True)
        return path

    def upload(self, path):
        self._update(root=path)
        return self.kaggle.dataset_create_version(path, version_notes=self._update(root=path), dir_mode='zip')
