##############################################################################
# test_insert_phm08_live.py
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
from cassandra.cluster import Cluster

cluster = Cluster()

session = cluster.connect('digilog_n')

my_insert = "INSERT INTO ctest(unit, cycle, sensor) VALUES (%s, %s, %s)"

session.execute(my_insert, [2,2,2])

insert_line = """INSERT INTO phm08_live (
                                unit,
                                cycle,
                                ct_timestamp,
                                op1,
                                op2,
                                op3,
                                sensor01,
                                sensor02,
                                sensor03,
                                sensor04,
                                sensor05,
                                sensor06,
                                sensor07, 
                                sensor08,
                                sensor09,
                                sensor10,
                                sensor11,
                                sensor12,
                                sensor13,
                                sensor14,
                                sensor15,
                                sensor16,
                                sensor17,
                                sensor18,
                                sensor19,
                                sensor20,
                                sensor21) VALUES (
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s)"""

l = [187, 122, 1603926416.149, 0.0019000000320374966, 0.0, 100.0, 518.6699829101562, 641.719970703125, 1587.4100341796875, 1395.6400146484375, 14.619999885559082, 21.610000610351562, 554.239990234375, 2388.030029296875, 9050.1796875, 1.2999999523162842, 47.470001220703125, 522.0599975585938, 2388.080078125, 8132.64990234375, 8.427499771118164, 0.029999999329447746, 390.0, 2388.0, 100.0, 38.959999084472656, 23.271900177001953]

session.execute(insert_line, l)
