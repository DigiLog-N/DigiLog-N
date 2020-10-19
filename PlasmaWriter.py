from pyarrow import array as pa_array
from pyarrow import record_batch, BufferReader, MockOutputStream, RecordBatchStreamReader, RecordBatchStreamWriter, FixedSizeBufferWriter
from PlasmaReader import PlasmaReader
import pyarrow.plasma as plasma
import numpy as np
from random import choice
from string import digits
from json import dumps
from time import time


class PlasmaWriter:
    def __init__(self, file_path, key_prefix):
        self.client = plasma.connect(file_path)

        #TODO: Revisit randomness for uniqueness during testing
        if len(key_prefix) > 14:
            raise ValueError("key_prefix should be less than 15 characters long, to leave enough room for random uniqueness.")

        self.key_prefix = key_prefix

    def _get_size(self, my_batch):
        mock_sink = MockOutputStream()
        stream_writer = RecordBatchStreamWriter(mock_sink, my_batch.schema)
        stream_writer.write_batch(my_batch)
        stream_writer.close()

        return mock_sink.size()

    def _write(self, my_batch):
        id_string = self.key_prefix + ''.join(choice(digits) for i in range(20 - len(self.key_prefix)))
        object_id = plasma.ObjectID(bytes(id_string, 'ascii'))

        buf = self.client.create(object_id, self._get_size(my_batch))

        stream_writer = RecordBatchStreamWriter(FixedSizeBufferWriter(buf), my_batch.schema)
        stream_writer.write_batch(my_batch)
        stream_writer.close()

        self.client.seal(object_id)

        return id_string

    def delete(self, list_of_keys):
        object_ids = [plasma.ObjectID(bytes(x, 'ascii')) for x in list_of_keys]
        self.client.delete(object_ids)


class NotifyWriter(PlasmaWriter):
    def __init__(self, file_path):
        super().__init__(file_path, 'NOTIFY')

    def write(self, recipients, message, subject):
        # because a single message w/subject can be sent to many individuals,
        # and because the number of elements in each array need to be the
        # same, we will need to turn the list of n recipients into a comma-
        # separated string, before wrapping it again as a list of one.
        epoch_timestamp = time()
        recipients = ','.join(recipients)
        my_data = [
            pa_array([epoch_timestamp]),
            pa_array([recipients]),
            pa_array([message]),
            pa_array([subject])
        ]

        rb = record_batch(my_data, names=['epoch_timestamp', 'recipients', 'message', 'subject'])
        id_string = self._write(rb)

        return id_string


if __name__ == '__main__':
    notify_writer = NotifyWriter('/tmp/plasma')

    recipients = []
    recipients.append('charlie@canvasslabs.com')
    recipients.append('unique.identifier@gmail.com')

    id_string = notify_writer.write(recipients, 'This is a new test', 'Test11')
