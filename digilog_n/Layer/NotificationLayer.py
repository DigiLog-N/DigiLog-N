##############################################################################
# NotificationLayer.py
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
from digilog_n.Log import log_file_main
from digilog_n.EMail import email_main
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
from digilog_n.Layer import Layer
import logging


mylogger = logging.getLogger("mylogger")
mylogger.info("TESTING")


class NotificationLayer(Layer):
    def __init__(self, path_to_plasma_file):
        super().__init__(path_to_plasma_file)
        self.name = 'Alert Module'
        self.ds_name = 'DigiLog-N Notifications'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'NOTIFY', remove_after_reading=True)

        while True:
            pdf = pr.to_pandas()
            if pdf is None:
                mylogger.debug("Alert Module: No new alerts")
                pass
            else:
                pdf = pdf.sort_values(by=['epoch_timestamp'])

                user = "user@gmail.com"
                password = "your_password_here"

                count = 0
                for index, row in pdf.iterrows():
                    recipients = row['recipients'].split(',')
                    subject = row['subject']
                    message = row['message']

                    email_main(user, password, subject, message, recipients)
                    count += 1
                    #log_file_main(user, password, subject, message, recipients)
                mylogger.info("Alert Module: Processing %d Alert Notifications. Sending out %d emails..." % (count, count))

            # sleep an arbitrary amount before checking for more notifications 
            # John and I agree that Plasma shouldn't have a problem polling at 1s intervals.
            sleep(3)
