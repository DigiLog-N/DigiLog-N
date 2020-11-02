from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
from subprocess import Popen, PIPE
from digilog_n.Layer import Layer
import logging


mylogger = logging.getLogger("mylogger")

class AnalysisLayer(Layer):
    def __init__(self):
        super().__init__()
        self.name = 'Analytical Engine'
        self.ds_name = 'PHM08 Prognostics Data Challenge Dataset'

    def run(self):
        self._before_you_begin()

        pr = PlasmaReader(self.plasma_path, 'PHM08')

        while True:
            mylogger.debug("Analytical Engine Layer: Checking for new PHM08 data...")

            latest_keys = pr.get_latest_keys()

            if latest_keys:
                # There are new data points for Spark to process
                mylogger.info("Analytical Engine Layer: New data found. Initiating 'Spark' AI Module...")
                object_ids = ' '.join(latest_keys)
                for object_id in object_ids:
                    mylogger.debug("New key: %s" % object_id)

                # Currently, RUL-Net does its own pulling of the data from Plasma.
                # Since additional keys may become available after we start RUL-Net,
                # We tell RUL-Net what the latest keys are, and it assembles a
                # history of each engine unit up to the point of the latest keys.
                # Anything newer should be ignored.

                # This allows us to control the lifetime of PHM08 data, as several
                # clients will be making use of it, including this program, Spark,
                # and Cassandra. 

                cmd_line = 'cd /home/charlie/RUL-Net_CL; . ./setvars.sh; . venv/bin/activate; ./run_spark.sh %s' % object_ids
                child = Popen(cmd_line, shell=True, stderr=PIPE, stdout=PIPE)
                out, err = child.communicate()
                l = out.decode('ascii').split('\n')
                l += err.decode('ascii').split('\n')
                for line in l:
                    mylogger.debug(line)
            else:
                mylogger.debug("No new PHM08 data")

            sleep(1)
