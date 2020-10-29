from digilog_n.PlasmaReader import PlasmaReader
from time import sleep
from subprocess import Popen, PIPE
from digilog_n.Layer import Layer
import logging
from cassandra.cluster import Cluster
import pandas as pd
from cassandra.concurrent import execute_concurrent_with_args


mylogger = logging.getLogger("mylogger")


class ArchiveLayer(Layer):
    def __init__(self):
        super().__init__()
        self.name = 'Archive'
        self.ds_name = 'PHM08 Prognostics Data Challenge Dataset'

    def run(self):
        self._before_you_begin()

        cluster = Cluster()
        session = cluster.connect('digilog_n')
        stmt = "INSERT INTO phm08_live ( unit, cycle, ct_timestamp, op1, op2, op3, sensor01, sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, sensor18, sensor19, sensor20, sensor21) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        prepared = session.prepare(stmt)

        pr = PlasmaReader(self.plasma_path, 'PHM08')

        while True:
            mylogger.info("Archive Layer checking for new PHM08 data...")

            pdf = pr.to_pandas()

            if pdf is not None:
                # pdf = pdf.sort_values(by=['epoch_timestamp'])
                for i in range(0, len(pdf)):
                    row = pdf.iloc[i]
                    ct_timestamp, unit, cycle, op1, op2, op3, s01, s02, s03, s04, s05, s06, s07, s08, s09, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21 = list(row.values)

                    unit = int(unit)
                    cycle = int(cycle)

                    # TODO: Re-examine execute_concurrent_with_args()
                    session.execute(prepared, (unit, cycle, ct_timestamp, op1, op2, op3, s01, s02, s03, s04, s05, s06, s07, s08, s09, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21)) 
            else:
                mylogger.debug("No new PHM08 data")
            sleep(1)
