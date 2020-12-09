##############################################################################
# NotifyWriter.py
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
    recipients.append('user1@gmail.com')
    recipients.append('user2@gmail.com')

    id_string = notify_writer.write(recipients, 'This is a new test.', 'test message')
