import pyarrow as pa
from PlasmaReader import PlasmaReader
from GMailUser import GMailUser
import pyarrow.plasma as plasma
import numpy as np
from random import choice
from string import digits


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
        object_id = plasma.ObjectID(bytes(id_string, 'ascii'))
        [data] = self.client.get_buffers([object_id], timeout_ms=0)
        if data:
            buffer = pa.BufferReader(data)
            # Convert object back into an Arrow RecordBatch
            reader = pa.RecordBatchStreamReader(buffer)
            table = reader.read_all()
            return table


if __name__ == '__main__':
    my_data = [
        pa.array(['unique.identifier@gmail.com']),
        pa.array(['This is a new test']),
        pa.array(['Test3'])
    ]

    my_batch = pa.record_batch(my_data, names=['recipient', 'message', 'subject'])
    write_to_plasma = WriteToPlasma()
    id_string = write_to_plasma.write(my_batch)
    table = write_to_plasma.read(id_string)

    def get_row(t, row_number):
        d = {}
        for column in t.schema.names:
            d[column] = str(t.column(column)[row_number])
        return d

    if table:
        d = get_row(table, 0)
        password = "D9AZm244L3UbYwBf"
        user = "cowartcharles1@gmail.com"

        recipients = []
        recipients.append(d['recipient'])
        email_main(user, password, d['subject'], d['message'], recipients)
