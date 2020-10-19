import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
import random
from random import choice
from string import digits

class WriteToPlasma:
    def __init__(self, plasma_file_path='/tmp/plasma'):
        self.client = plasma.connect(plasma_file_path)
        self.object_prefix = np.random.bytes(20)
        self.count = 0

    def write(self, batchA):
        # Write record to plasma
        # Create the Plasma object from the PyArrow RecordBatch. Most of the work here
        # is done to determine the size of buffer to request from the object store.
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(mock_sink, batchA.schema)
        stream_writer.write_batch(batchA)
        stream_writer.close()

        # Create an ObjectID.
        id_string = 'NOTIFY' + ''.join(choice(digits) for i in range(14))
        object_id = plasma.ObjectID(bytes(id_string, 'ascii'))
        buf = self.client.create(object_id, mock_sink.size())

        # Write the PyArrow RecordBatch to Plasma
        stream_writer = pa.RecordBatchStreamWriter(pa.FixedSizeBufferWriter(buf), batchA.schema)
        stream_writer.write_batch(batchA)
        stream_writer.close()

        # Seal the Plasma object
        self.client.seal(object_id)

        return id_string

    def read(self, id_string):
        # Read the batch from Plasma
        # Fetch the Plasma object

        #id_string = list(id_string)
        #id_string[0] = 'a'
        #id_string = ''.join(id_string)
        #print(id_string)

        object_id = plasma.ObjectID(bytes(id_string, 'ascii'))
        # Get the data from Plasma. We should already know the correct ID
        # so if it's not present, don't wait for it, timeout immediately.
        [data] = self.client.get_buffers([object_id], timeout_ms=0)  # Get PlasmaBuffer from ObjectID
        if data:
            buffer = pa.BufferReader(data)
            # Convert object back into an Arrow RecordBatch
            reader = pa.RecordBatchStreamReader(buffer)
            table = reader.read_all()
            print('\nAFrom plasma, column f0:\n')
            print(table.column("f0"))
            print('\nBFrom plasma, column f1:\n')
            print(table.column("f1"))
            print('\nCFrom plasma, column f2:\n')
            print(table.column("f2"))

if __name__ == '__main__':
    print("Hello World!")

    dataA = [
        pa.array([1, 2, 3, 4]),
        pa.array(['foo', 'bar', 'baz', None]),
        pa.array([True, None, False, True])
    ]

    batchA = pa.record_batch(dataA, names=['f0', 'f1', 'f2'])

    write_to_plasma = WriteToPlasma()
    id_string = write_to_plasma.write(batchA)

    write_to_plasma.read(id_string)


