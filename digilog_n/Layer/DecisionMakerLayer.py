from digilog_n.AnnotatePartsWriter import AnnotatePartsWriter
from digilog_n.RULResultReader import RULResultReader
from time import sleep
import pandas as pd
from digilog_n.Layer import Layer
import logging


mylogger = logging.getLogger("mylogger")


class DecisionMakerLayer(Layer):
    def __init__(self):
        super().__init__()
        self.name = 'Decision Maker'
        self.ds_name = ''

    def run(self):
        self._before_you_begin()

        rrr = RULResultReader(self.plasma_path, remove_after_reading=True)

        sleep_time = 3 

        while True:
            results = rrr.to_pandas()
            if results is None:
                mylogger.debug("No new results from Spark")
            else:
                for engine_unit in results:
                    self.decide(engine_unit, results[engine_unit])
                mylogger.debug("New results from Spark")

            mylogger.debug("sleeping %d seconds..." % sleep_time)
            # sleep an arbitrary amount before checking for more notifications 
            sleep(sleep_time)

    def decide(self, engine_unit, prediction_results):
        apw = AnnotatePartsWriter(self.plasma_path)

        latest_cycle = prediction_results.count()[0]
        unit_id = prediction_results.iloc[-1]['unit_id']
        prediction = prediction_results.iloc[-1]['rul_predict']

        # make a new DF with just the last row
        # append additional data to it as additional columns
        # create from scratch to bypass copy warning message.
        latest_result = {'unit_id': [unit_id], 'prediction': [prediction], 'current_cycle': [latest_cycle]}

        flag = None

        if prediction < 10:
            mylogger.info("CRITICAL: %d %d %f" % (unit_id, latest_cycle, prediction))
            flag = 'CRITICAL'
        elif prediction < 20:
            mylogger.info("DANGER: %d %d %f" % (unit_id, latest_cycle, prediction))
            flag = 'DANGER'
        elif prediction < 30:
            mylogger.info("RED: %d %d %f" % (unit_id, latest_cycle, prediction))
            flag = 'RED'
        elif prediction < 40:
            mylogger.info("ORANGE: %d %d %f" % (unit_id, latest_cycle, prediction))
            flag = 'ORANGE'
        elif prediction < 50:
            mylogger.info("YELLOW: %d %d %f" % (unit_id, latest_cycle, prediction))
            flag = 'YELLOW'

        if flag:
            latest_result['flag'] = [flag]
            latest_result = pd.DataFrame(latest_result)
            #mylogger.critical(latest_result.head())
            apw.from_pandas(latest_result)
            

