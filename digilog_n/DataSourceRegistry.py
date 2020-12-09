##############################################################################
# DataSourceRegistry.py
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
from digilog_n.DataSource import DataSource
import pymongo
import json


class DataSourceRegistry:
    def __init__(self, host, port, database, collection):
        self.client = pymongo.MongoClient("mongodb://%s:%d/" % (host, port))
        self.db = self.client[database]
        self.collection = self.db[collection]
        self.path_to_plasma = '/tmp/plasma'

    def get_path_to_plasma(self):
        return self.path_to_plasma

    def get_data_sources(self):
        results = []
        for doc in self.collection.find():
            ds = DataSource('', data=doc)
            results.append(ds)
        return results

    def get_data_source(self, name):
        for item in self.get_data_sources():
            if item.get_name() == name:
                return item

        return None

    def add_data_source(self, data_source):
        new_data_source = data_source.d

        # keep standard md5 hahes for _id values.
        # however, ensure that data-sources are unique by name.
        # do not add upsert support. Users shouldn't accidently and
        # unknowingly mess up an entry.
        for doc in self.collection.find():
            if doc['name'] == new_data_source['name']:
                raise ValueError("A data-source named '%s' already exists. Use update_data_source()." % new_data_source['name'])

        self.collection.insert_one(new_data_source)

    def remove_data_source(self, data_source):
        if isinstance(data_source, DataSource):
            query = { "name": data_source.d['name'] }
        else:
            query = {"name": data_source}

        # there shouldn't be more than one, but in case there is, it wouldn't
        # do to remove only one of them.
        self.collection.delete_many(query)

    def update_data_source(self, data_source):
        updated_data_source = data_source.d
        ds_name = updated_data_source['name']

        result = self.collection.replace_one({'name': ds_name}, updated_data_source)

        if result:
            # The result should be an UpdateObject. Assume successful update and return.
            return
        else:
            raise ValueError("A data-source named '%s' doesn't exist. Use add_data_source()." % ds_name)



