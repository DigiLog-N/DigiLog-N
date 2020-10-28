from digilog_n.Log import log_file_main
from digilog_n.EMail import email_main
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
from digilog_n.Layer import Layer
import logging


mylogger = logging.getLogger("mylogger")
mylogger.info("TESTING")


class NotificationLayer(Layer):
    def __init__(self):
        super().__init__()
        self.name = 'Notifications'
        self.ds_name = 'DigiLog-N Notifications'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'NOTIFY', remove_after_reading=True)

        count = 1

        while True:
            pdf = pr.to_pandas()
            if pdf is None:
                mylogger.debug("Notification Module: No new notifications")
                pass
            else:
                mylogger.info("Notification Module: New notifications")
                pdf = pdf.sort_values(by=['epoch_timestamp'])

                pdf.to_csv('/tmp/check%d.csv' % count)
                count += 1

                user = "cowartcharles1@gmail.com"
                password = "D9AZm244L3UbYwBf"

                for index, row in pdf.iterrows():
                    recipients = row['recipients'].split(',')
                    subject = row['subject']
                    message = row['message']

                    email_main(user, password, subject, message, recipients)
                    #log_file_main(user, password, subject, message, recipients)

            # sleep an arbitrary amount before checking for more notifications 
            # John and I agree that Plasma shouldn't have a problem polling at 1s intervals.
            sleep(3)
