##############################################################################
# register_notifications.py
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
import pymongo
import json
from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry


if __name__ == '__main__':
    mongo_client = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    ds = DataSource('DigiLog-N Notifications')
    ds.set_path_to_plasma_file('/tmp/plasma')
    ds.set_description('This dataset is a series of notifications issued by various layers/modules of the system.')
    ds.set_provider('CanvassLabs, Inc.')
    #name, units, precision, description, data_type
    ds.add_field('epoch_timestamp', 'seconds', 7, 'System-introduced epoch timestamp', 'double')
    ds.add_field('recipients', None, None, 'A comma-separated list of recipients', 'string')
    ds.add_field('message', None, None, 'A text or HTML-based message', 'string')
    ds.add_field('subject', None, None, 'A secondary text message', 'string')

    mongo_client.add_data_source(ds)

    data_sources = mongo_client.get_data_sources()

    for item in data_sources:
        print(item.json())
        #mongo_client.remove_data_source(item)
