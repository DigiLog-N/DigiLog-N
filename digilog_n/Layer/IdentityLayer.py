from digilog_n.NotifyWriter import NotifyWriter
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
from digilog_n.Layer import Layer
import logging
import pandas as pd


mylogger = logging.getLogger("mylogger")


class IdentityLayer(Layer):
    def __init__(self):
        super().__init__()
        self.name = 'Identity'
        self.ds_name = 'DigiLog-N Notifications'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'ANNO_GRPS', remove_after_reading=True)

        while True:
            result = pr.to_pandas()
            if result is None:
                mylogger.debug("No requests to annotate notifications with user groups")
            else:
                mylogger.info("Request to annotate notifications w/user groups")
                self.annotate(result)

            mylogger.debug("sleeping %d seconds..." % 3)

            sleep(3)

    def get_user_group(self, flag):
        d = {}
        d['YELLOW'] = ['charlie@canvasslabs.com']
        d['ORANGE'] = ['charlie@canvasslabs.com']
        d['RED'] = ['charlie@canvasslabs.com']
        d['DANGER'] = ['charlie@canvasslabs.com']
        d['CRITICAL'] = ['charlie@canvasslabs.com']

        '''
        d['YELLOW'] = ['charlie@canvasslabs.com', 'choonhan@canvasslabs.com', 'peter@canvasslabs.com', 'john.wilson@erigo.com']
        d['ORANGE'] = ['charlie@canvasslabs.com', 'choonhan@canvasslabs.com', 'peter@canvasslabs.com', 'john.wilson@erigo.com']
        d['RED'] = ['charlie@canvasslabs.com', 'choonhan@canvasslabs.com', 'peter@canvasslabs.com', 'john.wilson@erigo.com']
        d['DANGER'] = ['charlie@canvasslabs.com', 'choonhan@canvasslabs.com', 'peter@canvasslabs.com', 'john.wilson@erigo.com']
        d['CRITICAL'] = ['charlie@canvasslabs.com', 'choonhan@canvasslabs.com', 'peter@canvasslabs.com', 'john.wilson@erigo.com']
        '''

        return d[flag]


    def annotate(self, result):
        nw = NotifyWriter(self.plasma_path)

        mylogger.info(result.head())

        metadata = result[['unique_id', 'unit_id', 'prediction', 'current_cycle', 'flag']].copy().drop_duplicates()
        mylogger.info(metadata.head(100))

        for i in range(0, len(metadata)):
            message = []

            unique_id = metadata.iloc[i][0]
            unit_id = metadata.iloc[i][1]
            prediction = metadata.iloc[i][2]
            current_cycle = metadata.iloc[i][3]
            flag = metadata.iloc[i][4]

            message.append("Engine Unit ID: %d" % unit_id)
            message.append("RUL Prediction: %f" % prediction)
            message.append("Current Engine Cycle: %d" % current_cycle)
            message.append("Warning Level: %s" % flag)
            message.append("Parts Ordered:")

            parts = result.loc[result['unique_id'] == unique_id]

            d = {}
            for j in range(0, len(parts)):
                part = parts.iloc[j][5]
                location = parts.iloc[j][6]
                qty_available = parts.iloc[j][7]
                qty_requested = parts.iloc[j][8]

                if part not in d:
                    d[part] = []

                d[part].append((location, qty_available, qty_requested))

            for part in d:
                message.append("\t%s" % part)
                for location, qty_available, qty_requested in d[part]:
                    message.append("\t\tLocation: %s" % location)
                    message.append("\t\tQty Available: %s" % qty_available)
                    message.append("\t\tQty Requested: %s" % qty_requested)
                message.append("")

            subject = 'Engine Unit %d: %s' % (unit_id, flag)
            nw.write(self.get_user_group(flag), '\n'.join(message), subject)
