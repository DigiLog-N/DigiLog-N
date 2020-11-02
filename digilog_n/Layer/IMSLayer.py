from digilog_n.AnnotateGroupsWriter import AnnotateGroupsWriter
from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
import pandas as pd
from digilog_n.Layer import Layer
import logging
from uuid import uuid4


mylogger = logging.getLogger("mylogger")


class IMSLayer(Layer):
    def __init__(self, path_to_plasma_file):
        super().__init__(path_to_plasma_file)
        self.name = 'Inventory Management System'
        self.ds_name = 'DigiLog-N Notifications'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'ANNO_PRTS', remove_after_reading=True)

        while True:
            result = pr.to_pandas()
            if result is None:
                mylogger.debug("Inventory Management System Layer: No requests to annotate notifications with parts lists")
            else:
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

        results = []
        for part_name in part_set[flag]:
            for location in part_locations[part_name]:
                row = []
                row.append(part_name)
                row.append(location)
                row.append(part_locations[part_name][location])
                row.append(1)
                results.append(row)

        return results

    def annotate(self, result):
        agw = AnnotateGroupsWriter(self.plasma_path)

        #mylogger.info(result.head())
        #TODO: cleanup later
        count = 0

        for index, row in result.iterrows():
            unit_id = row['unit_id']
            prediction = row['prediction']
            current_cycle = row['current_cycle']
            flag = row['flag']
            count += 1

            # Create an eight column-wide DataFrame. Set up the first four
            # columns, which will be repeated through all n rows:
            prepend = [unit_id, prediction, current_cycle, flag]

            # Get the parts-set associated with this flag. get_parts()
            # munges the parts-set with the location and quantity of each
            # part, and results a tablular-formatted result.
            results = self.get_parts(flag)

            # create the new table as a list of rows
            tbl = []
            for row in results:
                tbl.append(prepend + row)

            # before turning it into a DataFrame, it needs to be turned
            # into columns.

            uid = str(uuid4())

            results = { 'unique_id': [uid for x in tbl], 
                        'unit_id': [x[0] for x in tbl],
                        'prediction': [x[1] for x in tbl],
                        'current_cycle' : [x[2] for x in tbl],
                        'flag': [x[3] for x in tbl],
                        'part': [x[4] for x in tbl],
                        'location': [x[5] for x in tbl],
                        'qty_available': [x[6] for x in tbl],
                        'qty_requested': [x[7] for x in tbl]}

            # push the new DataFrame into the system
            agw.from_pandas(pd.DataFrame(results))

        mylogger.info("Inventory Management System Layer: Annotating %d Alert Requests with Inventory Information..." % count)
