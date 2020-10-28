from digilog_n.NotifyWriter import NotifyWriter
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
from digilog_n.Layer import Layer
import logging


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
                mylogger.info("No requests to annotate notifications with user groups")
            else:
                mylogger.info("Request to annotate notifications w/user groups")
                self.annotate(result)

            mylogger.debug("sleeping %d seconds..." % 3)

            sleep(3)

    def annotate(self, result):
        nw = NotifyWriter(self.plasma_path)

        mylogger.info(result.head())

        l = []

        for index, row in result.iterrows():
            l.append("Engine Unit ID: %d" % row['unit_id'])
            l.append("RUL Prediction: %f" % row['prediction'])
            l.append("Current Engine Cycle: %d" % row['current_cycle'])
            l.append("Warning Level: %s" % row['flag'])
            l.append("Parts Ordered:")
            for part in row['part_set']:
                l.append("\t%s" % part)

            message = '\n'.join(l)

            nw.write(['ucsdboy@gmail.com', 'unique.identifier@gmail.com'], message, 'new results from spark')
