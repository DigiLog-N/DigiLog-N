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


class PlasmaReader:
    def __init__(self, file_path):
        self.client = plasma.connect(file_path)
        # self._get_keys()
        self.d = {}
        self.keys_read = []

    def get_keys(self):
        # generates a list of keys to objects currently in the Plasma store.
        # this method is called at construction, and the list of keys can
        # be refreshed any time this method is called by the user.
        l = list(self.client.list().keys())

        # currently the only way I know to remove the ObjectID description
        # from the id itself.
        l = [str(x).replace('ObjectID(', '').rstrip(')') for x in l]

        # the idata_source in l are encoded in hex, and thus not human-readable. This
        # will allow us to get a human-browsable list.
        l = [bytearray.fromhex(str(x)).decode() for x in l]

        return l

    def _read_from_key(self, key):
        # Fetch one object from Plasma
        id = plasma.ObjectID(key.encode())

        # Read data from the Plasma object
        # Each Plasma object written out from CT2Arrow only contains 1 record batch
        # Examine the data from this record batch
        # (see https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch)
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
        reader = pa.RecordBatchStreamReader(pa.BufferReader(data))

        batch = reader.read_next_batch()
        #print('number of columns = %d' %(batch.num_columns))
        #print('number of rows = %d' % (batch.num_rows))
        #print('schema:')
        # generate Cassandra schema from the metadata output from this method.
        print(batch.schema)
        header = batch.schema.names

        for i in range(0, batch.num_columns):
            column = header[i]
            if not column in self.d:
                self.d[header[i]] = []

            self.d[header[i]] += batch.column(i).to_pylist()

        # note that 60 rows of data for Engine Unit 38 is in PHM08********_b00102,
        # while the final 29 is in PHM08********_b00103. Each Object contains
        # data for only one Engine Unit (Confirmed). Hence, final confirmation
        # requires iterating through all Objects.
        self.keys_read.append(key)

    def read_all_columns(self):
        # any key successfully read by _read_from_key() will append the
        # key to self.keys_read. This set difference operation will ensure
        # only keys that haven't already been processed successfully will be
        # read.
        latest_keys = list(set(self.get_keys()) - set(self.keys_read))

        for key in latest_keys:
            self._read_from_key(key)

        return self.d


if __name__ == '__main__':
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    data_source = dsr.get_data_source('PHM08 Prognostics Data Challenge Dataset')

    if data_source:
        # Readers specific to a particular data-source could be sub-classed from
        # PlasmaReader. Currently, this PlasmaReader rips all columns and stores
        # them as lists in an internal dictionary. You can access all of 'op3'
        # for instance by simply using as below. At some time interval, you can
        # read_all_columns() again, and PlasmaReader will determine which buffers
        # are new, extract their data, mark the buffer names off in its internal
        # list, and append the data to the existing data structure.
        #
        # the user may not want to preserve their own copy of the data in the
        # data structure, hence one could simply delete self.d and reinitialize
        # it to {}, etc. There's a number of possibilities, depending on
        # the usage patterns needed for generating models in pyspark.
        #
        # I also have some code for loading this data into a DataFrame, but I
        # think this might be heavyweight for our use-case.
        pr = PlasmaReader(data_source.get_path_to_plasma_file())
        results = pr.read_all_columns()
        print("There should be 619 results in op3 right now.")
        print(len(results['op3']))

        fields = data_source.get_fields()
        for field in fields:
            print(field)
            print(fields[field])
            print(results[field])
            print("")
