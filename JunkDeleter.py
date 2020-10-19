# Charles Cowart, Canvass Labs, Inc.
# based on work by John P. Wilson, Erigo Technologies
import json
import numpy as np
import pyarrow as pa
import pyarrow.plasma as plasma
import pymongo
import sys
from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry
import pandas as pd


class JunkDeleter:
    def __init__(self, file_path, key_prefix):
        self.client = plasma.connect(file_path)
        self.keys_read = []
        self.key_prefix = key_prefix

    def delete_keys(self):
        l = list(self.client.list().keys())

        delete_these = []

        for obj in l:
            s = str(obj)
            if 'df8b7c3c5388888929099abd55f2d53f3fb91a3b' in s:
                delete_these.append(obj)

        self.client.delete(delete_these)




if __name__ == '__main__':
    jd = JunkDeleter('/tmp/plasma', 'foo')
    l = jd.delete_keys()

