##############################################################################
# ArchiveLayer.py
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
from digilog_n.PlasmaReader import PlasmaReader
from digilog_n.RULResultReader import RULResultReader
from time import sleep
from subprocess import Popen, PIPE
from digilog_n.Layer import Layer
import logging
from cassandra.cluster import Cluster
import pandas as pd
from cassandra.concurrent import execute_concurrent_with_args


mylogger = logging.getLogger("mylogger")


class ArchiveLayer(Layer):
    def __init__(self, path_to_plasma_file):
        super().__init__(path_to_plasma_file)
        self.name = 'DB Query & Archive'
        self.ds_name = 'PHM08 Prognostics Data Challenge Dataset'

    def run(self):
        self._before_you_begin()

        cluster = Cluster()
        session = cluster.connect('digilog_n')

        stmt = "INSERT INTO phm08_live ( unit, cycle, ct_timestamp, op1, op2, op3, sensor01, sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, sensor18, sensor19, sensor20, sensor21) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        prepared = session.prepare(stmt)

        stmt2 = "INSERT INTO weather_live ( station_id, ct_timestamp, date_time, altimeter_set_1, air_temp_set_1, dew_point_temperature_set_1, relative_humidity_set_1, wind_speed_set_1, wind_direction_set_1, wind_gust_set_1, sea_level_pressure_set_1, weather_cond_code_set_1, cloud_layer_3_code_set_1, pressure_tendency_set_1, precip_accum_one_hour_set_1, precip_accum_three_hour_set_1, cloud_layer_1_code_set_1, cloud_layer_2_code_set_1, precip_accum_six_hour_set_1, precip_accum_24_hour_set_1, visibility_set_1, metar_remark_set_1, metar_set_1, air_temp_high_6_hour_set_1, air_temp_low_6_hour_set_1, peak_wind_speed_set_1, ceiling_set_1, pressure_change_code_set_1, air_temp_high_24_hour_set_1, air_temp_low_24_hour_set_1, peak_wind_direction_set_1, dew_point_temperature_set_1d, cloud_layer_1_set_1d, cloud_layer_3_set_1d, cloud_layer_2_set_1d, wind_chill_set_1d, weather_summary_set_1d, wind_cardinal_direction_set_1d, pressure_set_1d, sea_level_pressure_set_1d, heat_index_set_1d, weather_condition_set_1d ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        prepared2 = session.prepare(stmt2)

        stmt3 = "INSERT INTO rul_result_live ( unit, cycle, rul_estimate ) VALUES ( ?, ?, ? )"
        prepared3 = session.prepare(stmt3)

        pr = PlasmaReader(self.plasma_path, 'PHM08')
        pr2 = PlasmaReader(self.plasma_path, 'KSAN')
        rrr = RULResultReader(self.plasma_path, remove_after_reading=False)

        while True:
            mylogger.debug("DB Query & Archive: checking for new PHM08 data...")

            pdf = pr.to_pandas()

            if pdf is not None:
                mylogger.info("DB Query & Archive: results found for PHM08 data...")
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

            mylogger.debug("DB Query & Archive: checking for new KSAN data...")

            pdf2 = pr2.to_pandas()

            if pdf2 is not None:
                mylogger.info("DB Query & Archive: results found for KSAN data...")
                # pdf2 = pdf2.sort_values(by=['epoch_timestamp'])
                for i in range(0, len(pdf2)):
                    row = pdf2.iloc[i]
                    mylist = list(row.values)
                    if len(mylist) == 42:
                        ct_timestamp, date_time, station_id, altimeter_set_1, air_temp_set_1, dew_point_temperature_set_1, relative_humidity_set_1, wind_speed_set_1, wind_direction_set_1, wind_gust_set_1, sea_level_pressure_set_1, weather_cond_code_set_1, cloud_layer_3_code_set_1, pressure_tendency_set_1, precip_accum_one_hour_set_1, precip_accum_three_hour_set_1, cloud_layer_1_code_set_1, cloud_layer_2_code_set_1, precip_accum_six_hour_set_1, precip_accum_24_hour_set_1, visibility_set_1, metar_remark_set_1, metar_set_1, air_temp_high_6_hour_set_1, air_temp_low_6_hour_set_1, peak_wind_speed_set_1, ceiling_set_1, pressure_change_code_set_1, air_temp_high_24_hour_set_1, air_temp_low_24_hour_set_1, peak_wind_direction_set_1, dew_point_temperature_set_1d, cloud_layer_1_set_1d, cloud_layer_3_set_1d, cloud_layer_2_set_1d, wind_chill_set_1d, weather_summary_set_1d, wind_cardinal_direction_set_1d, pressure_set_1d, sea_level_pressure_set_1d, heat_index_set_1d, weather_condition_set_1d = list(row.values)
                        weather_cond_code_set_1 = int(weather_cond_code_set_1)
                        session.execute(prepared2, (station_id, ct_timestamp, date_time, altimeter_set_1, air_temp_set_1, dew_point_temperature_set_1, relative_humidity_set_1, wind_speed_set_1, wind_direction_set_1, wind_gust_set_1, sea_level_pressure_set_1, weather_cond_code_set_1, cloud_layer_3_code_set_1, pressure_tendency_set_1, precip_accum_one_hour_set_1, precip_accum_three_hour_set_1, cloud_layer_1_code_set_1, cloud_layer_2_code_set_1, precip_accum_six_hour_set_1, precip_accum_24_hour_set_1, visibility_set_1, metar_remark_set_1, metar_set_1, air_temp_high_6_hour_set_1, air_temp_low_6_hour_set_1, peak_wind_speed_set_1, ceiling_set_1, pressure_change_code_set_1, air_temp_high_24_hour_set_1, air_temp_low_24_hour_set_1, peak_wind_direction_set_1, dew_point_temperature_set_1d, cloud_layer_1_set_1d, cloud_layer_3_set_1d, cloud_layer_2_set_1d, wind_chill_set_1d, weather_summary_set_1d, wind_cardinal_direction_set_1d, pressure_set_1d, sea_level_pressure_set_1d, heat_index_set_1d, weather_condition_set_1d))
                    else:
                        mylogger.error(mylist)
            else:
                mylogger.debug("No new KSAN data")

            mylogger.debug("DB Query & Archive: checking for new RUL_RSLT data...")

            results_dict = rrr.to_pandas()

            if results_dict is not None:
                mylogger.info("DB Query & Archive: results found for %d engines..." % len(results_dict))
                for engine_unit in results_dict:
                    results = results_dict[engine_unit]
                    cycle = results.count()[0]
                    # prediction = results.iloc[-1]['rul_predict']
                    # session.execute(prepared3, (engine_unit, cycle, prediction))
                    for index, row in results.iterrows():
                        prediction = row['rul_predict']
                        cycle = index + 1
                        session.execute(prepared3, (engine_unit, cycle, prediction))
            else:
                mylogger.debug("No new RUL Results")

            sleep(3)
