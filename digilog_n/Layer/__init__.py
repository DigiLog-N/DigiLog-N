##############################################################################
# __init__.py
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
from multiprocessing import Process
from sys import stdout
import logging


mylogger = logging.getLogger("mylogger")


class Layer(Process):
    def __init__(self, path_to_plasma_file):
        super().__init__()
        self.name = ''
        self.dsr = None
        self.ds_name = None
        self.plasma_path = path_to_plasma_file

    def _before_you_begin(self):
        mylogger.info('%s Layer: Starting...' % self.name)

        #self.dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'self.data_sources')

        #self.data_source = self.dsr.get_data_source('DigiLog-N Notifications')  #self.ds_name)

        #mylogger.info("%s Layer: Getting '%s' from Plasma..." % (self.name, self.ds_name))

        #if not self.data_source:
        #    mylogger.error("%s Layer: Could not locate data-source: %s" % (self.name, self.ds_name))
        #    exit(1)
