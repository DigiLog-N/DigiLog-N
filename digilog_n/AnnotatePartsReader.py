##############################################################################
# AnnotatePartsReader.py
# https://github.com/DigiLog-N/DigiLog-N
# Copyright 2020 Canvass Labs, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################
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


class AnnotatePartsReader(PlasmaReader):
    def __init__(self, file_path, remove_after_reading=False):
        self.client = plasma.connect(file_path)
        self.keys_read = []
        self.key_prefix = 'ANNO_PRTS'
        self.auto_remove = remove_after_reading 

    def _get_keys(self):
        # Generates a list of keys to objects currently in the Plasma store.
        # PlasmaReader object keeps a list of keys read. 
        # Read the data associated with each key once and only once.
        # Once the value of a key has been 'sealed', it becomes read-only and
        # Available to clients. Hence, we can discover new rows by obtaining
        # the list of all keys and taking the set difference with the list of
        # keys already read.,
        #
        # At some point, we need an option to delete keys that we have read.
        # assuming no other client needs to read them.
        l = list(self.client.list().keys())

        # currently the only way I know to remove the ObjectID description
        # from the id itself.
        l = [str(x).replace('ObjectID(', '').rstrip(')') for x in l]

        # the idata_source in l are encoded in hex, and thus not human-readable. This
        # will allow us to get a human-browsable list.
        l = [bytearray.fromhex(str(x)).decode() for x in l]

        l = [x for x in l if x.startswith(self.key_prefix)]

        return l

    def _read_from_key(self, key):
        # Fetch one object from Plasma
        id = plasma.ObjectID(key.encode())

        # Read data from the Plasma object
        # Each Plasma object written out from CT2Arrow only contains 1 record batch
        #
        # buffers are created in a two-step process that first sees memory allocated and the buffer gets
        #  populated with data. The second step is when the object is 'sealed', making it read-only and
        #  available to all other clients. You can be assured that when you read a buffer, you will never
        #  have to re-read it. Thus, you only need to keep track of what buffers you've read in order to
        #  get new data as it becomes available.
        #
        # For this version, we will call get_buffers(id) w/out a timeout; it will block until the buffer
        #  is sealed. It keeps things simple; we've either read it, or we haven't.
        [data] = self.client.get_buffers([id])

        # Fetch the Plasma object and convert object back into an Arrow RecordBatch
        reader = pa.RecordBatchStreamReader(pa.BufferReader(data))
        # Remember, only one batch per object.
        batch = reader.read_next_batch()
        #print('number of columns = %d' %(batch.num_columns))
        #print('number of rows = %d' % (batch.num_rows))
        #print('schema:')
        header = batch.schema.names

        if self.auto_remove:
            self.client.delete([id])

        self.keys_read.append(key)

        return batch

    def to_pandas(self):
        # any key successfully read by _read_from_key() will append the
        # key to self.keys_read. This set difference operation will ensure
        # only keys that haven't already been processed successfully will be
        # read.
        latest_keys = list(set(self._get_keys()) - set(self.keys_read))

        if latest_keys:
            latest_keys.sort()
            # return just the first of the latest_keys found
            # and mark it read. one df per request this time.
            key = latest_keys[0]
            self._read_from_key(key)
            table = pa.Table.from_batches([self._read_from_key(key)])
            return table.to_pandas()

        return None

    def get_latest_keys(self):
        latest_keys = list(set(self._get_keys()) - set(self.keys_read))

        if latest_keys:
            latest_keys.sort()
            self.keys_read += latest_keys
            return latest_keys

        return None
