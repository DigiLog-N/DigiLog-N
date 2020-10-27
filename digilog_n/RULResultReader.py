# Charles Cowart, Canvass Labs, Inc.
# based on work by John P. Wilson, Erigo Technologies
from digilog_n.DataSource import DataSource
from digilog_n.DataSourceRegistry import DataSourceRegistry
from digilog_n.PlasmaReader import PlasmaReader
import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import pymongo
import sys


class RULResultReader(PlasmaReader):
    def __init__(self, file_path, remove_after_reading=False):
        # TODO: Use super
        self.client = plasma.connect(file_path)
        self.keys_read = []
        self.key_prefix = 'RUL_RSLT'
        self.auto_remove = remove_after_reading 

    def to_pandas(self):
        '''
        instead of returning a data-frame with just the index number and the
        result, let's return a dictionary of data frames, where the key is
        the name of the engine unit, and the value is the dataframe for all
        values up to the current point.

        Instead of aggregating data across multiple keys, RUL_RESULT objects
        need to be processed individually, as each one represents the results
        for a single engine unit. The unit number is reflected in the key,
        hence it's important to extract that metadata from the key as well.
        '''
        # any key successfully read by _read_from_key() will append the
        # key to self.keys_read. This set difference operation will ensure
        # only keys that haven't already been processed successfully will be
        # read.
        latest_keys = list(set(self._get_keys()) - set(self.keys_read))

        results_by_unit = {}

        for key in latest_keys:
            table = pa.Table.from_batches([self._read_from_key(key)])
            results_by_unit[key] = table.to_pandas()

        if results_by_unit:
            return results_by_unit

        return None
