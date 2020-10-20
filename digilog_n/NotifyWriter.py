from pyarrow import array as pa_array
from pyarrow import record_batch
from digilog_n.PlasmaWriter import PlasmaWriter
from time import time


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

    id_string = notify_writer.write(recipients, 'This is a new test', 'Test12')
