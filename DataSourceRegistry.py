import pymongo
import json
from DataSource import DataSource


class DataSourceRegistry:
    def __init__(self, host, port, database, collection):
        self.client = pymongo.MongoClient("mongodb://%s:%d/" % (host, port))
        self.db = self.client[database]
        self.collection = self.db[collection]

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

        # I want to keep '_id' values for objects standard.
        # However, I still want data-sources to be unique by name.
        # Hence, we check to see if a data-source with a matching name already exists.
        #
        # For now, let's not support upsert() behavior. We don't want users accidently
        # overwriting entries.
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



