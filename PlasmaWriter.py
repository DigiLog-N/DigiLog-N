from pyarrow import MockOutputStream, RecordBatchStreamWriter, FixedSizeBufferWriter
import pyarrow.plasma as plasma
from random import choice
from string import digits


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
