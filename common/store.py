import os

import glob as gb
import pandas as pd
import pystore as db


class Store(object):
    def __init__(self, name, root, config, subreddit):
        self.name = name
        self.root = root
        self.config = config
        self.subreddit = subreddit

        # datastore
        db.set_path(os.path.join(self.root, 'data', 'store'))
        self.datastore = db.store(self.subreddit, 'fastparquet')

        # collection
        self.collection = self.datastore.collection(self.name)
        self.path = os.path.join(self.datastore.datastore, self.collection.collection)

    def exists_meta(self, name):
        path = os.path.join(self.path, name)
        return any(gb.glob(os.path.join(path, '*.json')))

    def read_meta(self, name):
        if self.exists_meta(name):
            return db.utils.read_metadata(os.path.join(self.path, name))
        return {}

    def write_meta(self, name, **kwargs):
        path = os.path.join(self.path, name)
        os.makedirs(path, exist_ok=True)
        db.utils.write_metadata(path, metadata=kwargs)

    def exists_data(self, name):
        path = os.path.join(self.path, name)
        return any(gb.glob(os.path.join(path, '*.parquet')))

    def read_data(self, name):
        if self.exists_data(name):
            return self.collection.item(name).to_pandas(parse_dates=False)
        return pd.DataFrame()

    def write_data(self, name, data, overwrite, **kwargs):
        if self.exists_data(name) or overwrite:
            self.collection.write(name, data, overwrite=True, metadata=kwargs)
        else:
            self.collection.append(name, data)
            self.write_meta(name, **kwargs)
