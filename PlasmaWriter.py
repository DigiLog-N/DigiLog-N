from pyarrow import array as pa_array
from pyarrow import record_batch, BufferReader, MockOutputStream, RecordBatchStreamReader, RecordBatchStreamWriter, FixedSizeBufferWriter
from PlasmaReader import PlasmaReader
import pyarrow.plasma as plasma
import numpy as np
from random import choice
from string import digits
from GMailUser import GMailUser
from json import dumps
from time import time


def email_main(user, password, subject, message, addressed_to):
    try:
        email_alert = GMailUser(user, password, verify_parameters=False)
        email_alert.send(subject, message, addressed_to, hangup=True)
    except SMTPHeloError as e:
        raise ValueError("I said HELO, but the server ignored me: %s" % str(e))
    except SMTPAuthenticationError as e:
        raise ValueError("The SMTP server did not accept your username and/or password: %s" % str(e))
    except SMTPNotSupportedError as e:
        raise ValueError("The AUTH and/or SMTPUTF8 command is not supported by this server: %s" % str(e))
    except SMTPException as e:
        raise ValueError("A suitable authentication method couldn't be found: %s" % str(e))
    except gaierror as e:
        raise ValueError("Invalid SMTP host name: %s" % str(e))
    except RuntimeError as e:
        raise EnvironmentError("TLS and/or SSL support is not available to your Python interpreter: %s" % str(e))
    except SMTPRecipientsRefused as e:
        raise ValueError("All recipients were refused. Please verify your list of recipients is correct.")
    except SMTPSenderRefused as e:
        raise ValueError("Sender refused From=%s: %s" % (user, str(e)))
    except SMTPDataError as e:
        # TODO: This should probably be a different kind of error.
        #  consider wrapping all errors in a digilog-N specific error type.
        #  catching all of these errors here and re-raising them as a merged set of types is mainly
        #  to prevent downstream code from having to know/handle all of these very specific errors.
        #  Yet, we want to be fairly robust; we need to know if a user did not receive an email.
        raise ValueError("Server replied w/unexpected error code: %s" % str(e))


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

    def read(self, id_string):
        object_id = plasma.ObjectID(bytes(id_string, 'ascii'))
        [data] = self.client.get_buffers([object_id], timeout_ms=0)
        if data:
            buffer = BufferReader(data)
            # Convert object back into an Arrow RecordBatch
            reader = RecordBatchStreamReader(buffer)
            table = reader.read_all()
            return table

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

    id_string = notify_writer.write(recipients, 'This is a new test', 'Test10')

    table = notify_writer.read(id_string)

    def get_row(t, row_number):
        d = {}
        for column in t.schema.names:
            d[column] = str(t.column(column)[row_number])
        return d

    if table:
        password = "D9AZm244L3UbYwBf"
        user = "cowartcharles1@gmail.com"
        for i in range(0, table.num_rows):        
            d = get_row(table, 0)

            print(d)

            #d['recipients'] = d['recipients'].split(',')

            #email_main(user, password, d['subject'], d['message'], d['recipients'])
