from digilog_n.AnnotateGroupsWriter import AnnotateGroupsWriter
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
import pandas as pd
from digilog_n.Layer import Layer
import logging


mylogger = logging.getLogger("mylogger")


class FakeIMSLayer(Layer):
    def __init__(self, inventory_csv_path):
        super().__init__()
        self.name = 'Fake IMS'
        mylogger.info('Fake IMS: Using Inventory File: %s...' % inventory_csv_path)
        self.parts_inventory = pd.read_csv(inventory_csv_path)
        mylogger.info(self.parts_inventory.head())
        self.ds_name = 'DigiLog-N Notifications'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'ANNO_PRTS', remove_after_reading=True)

        while True:
            result = pr.to_pandas()
            if result is None:
                mylogger.debug("No requests to annotate notifications with parts lists")
            else:
                mylogger.debug("Request to annotate notifications w/parts lists")
                self.annotate(result)

            mylogger.debug("sleeping %d seconds..." % 3)

            sleep(3)

    def annotate(self, result):
        agw = AnnotateGroupsWriter(self.plasma_path)

        mylogger.info(result.head())

        part_set = {}
        part_set['CRITICAL'] = ['PartA', 'PartB', 'PartC', 'PartD', 'PartE', 'PartF', 'PartG']
        part_set['DANGER'] = ['PartA', 'PartB', 'PartC', 'PartD', 'PartE', 'PartF']
        part_set['RED'] = ['PartA', 'PartB', 'PartC', 'PartD', 'PartE']
        part_set['ORANGE'] = ['PartA', 'PartB', 'PartC', 'PartD']
        part_set['YELLOW'] = ['PartA', 'PartB', 'PartC']

        for index, row in result.iterrows():
            unit_id = row['unit_id']
            prediction = row['prediction']
            current_cycle = row['current_cycle']
            flag = row['flag']

            result = {  'unit_id': [unit_id],
                        'prediction': [prediction],
                        'current_cycle': [current_cycle],
                        'flag': [flag],
                        'part_set': [part_set[flag]] }

            agw.from_pandas(pd.DataFrame(result))
