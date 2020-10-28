from pyarrow import RecordBatch, MockOutputStream, RecordBatchStreamWriter, FixedSizeBufferWriter
import pyarrow.plasma as plasma
from random import choice
from string import digits
from digilog_n.PlasmaWriter import PlasmaWriter


class AnnotatePartsWriter(PlasmaWriter):
    def __init__(self, file_path):
        self.client = plasma.connect(file_path)
        self.key_prefix = 'ANNO_PRTS'

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

    def from_pandas(self, pdf):
        record_batch = RecordBatch.from_pandas(pdf)

        return self._write(record_batch)
