import os

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

    def read_meta(self, name):
        if name in self.collection.list_items():
            return db.utils.read_metadata(os.path.join(self.path, name))
        return {}

    def write_meta(self, name, **kwargs):
        path = os.path.join(self.path, name)
        os.makedirs(path, exist_ok=True)
        db.utils.write_metadata(path, metadata=kwargs)

    def read_data(self, name):
        if name in self.collection.list_items():
            return self.collection.item(name).to_pandas(parse_dates=False)
        return pd.DataFrame()

    def write_data(self, name, data, overwrite, **kwargs):
        if name not in self.collection.list_items() or overwrite:
            self.collection.write(name, data, overwrite=True, metadata=kwargs)
        else:
            self.collection.append(name, data)
            self.write_meta(name, **kwargs)
