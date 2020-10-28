from digilog_n.AnnotateGroupsWriter import AnnotateGroupsWriter
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
import pandas as pd
from digilog_n.Layer import Layer
import logging


mylogger = logging.getLogger("mylogger")


class IMSLayer(Layer):
    def __init__(self):
        super().__init__()
        self.name = 'IMS'
        self.ds_name = 'DigiLog-N Notifications'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'ANNO_PRTS', remove_after_reading=True)

        while True:
            result = pr.to_pandas()
            if result is None:
                mylogger.debug("No requests to annotate notifications with parts lists")
            else:
                mylogger.info("Request to annotate notifications w/parts lists")
                self.annotate(result)

            mylogger.debug("sleeping %d seconds..." % 3)

            sleep(3)

    def get_parts(self, flag):
        part_set = {}
        part_set['CRITICAL'] = ['PartA', 'PartB', 'PartC', 'PartD', 'PartE', 'PartF', 'PartG']
        part_set['DANGER'] = ['PartA', 'PartB', 'PartC', 'PartD', 'PartE', 'PartF']
        part_set['RED'] = ['PartA', 'PartB', 'PartC', 'PartD', 'PartE']
        part_set['ORANGE'] = ['PartA', 'PartB', 'PartC', 'PartD']
        part_set['YELLOW'] = ['PartA', 'PartB', 'PartC']

        part_locations = {}
        part_locations['PartA'] = {'LocationD': 2, 'LocationA': 56}
        part_locations['PartB'] = {'LocationA': 1, 'LocationK': 5}
        part_locations['PartC'] = {'LocationE': 57, 'LocationG': 6}
        part_locations['PartD'] = {'LocationK': 99, 'LocationP': 9}
        part_locations['PartE'] = {'LocationU': 45, 'LocationO': 107}
        part_locations['PartF'] = {'LocationD': 21, 'LocationR': 88}
        part_locations['PartG'] = {'LocationY': 7, 'LocationL': 23}

        foo = []

        for item in part_set[flag]:
            d = {}
            d['part_name'] = item
            d['locations_found'] = part_locations[item]
            foo.append(d)

        return foo

    def annotate(self, result):
        agw = AnnotateGroupsWriter(self.plasma_path)

        mylogger.info(result.head())

        for index, row in result.iterrows():
            unit_id = row['unit_id']
            prediction = row['prediction']
            current_cycle = row['current_cycle']
            flag = row['flag']

            parts = self.get_parts(flag)

            result = {  'unit_id': [unit_id],
                        'prediction': [prediction],
                        'current_cycle': [current_cycle],
                        'flag': [flag],
                        'part_set': [parts] }

            agw.from_pandas(pd.DataFrame(result))
