import pymongo
import json


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

        self.collection.delete_many(query)






class DataSource:
    def __init__(self, name, data=None):
        if data:
            del (data['_id'])
            self.d = data
        else:
            self.d = {'name': name}
            self.d['fields'] = {}
            # self.d['field_order'] = []
            self.d['path_to_plasma_file'] = None
            self.d['description'] = None
            self.d['provider'] = None



    def set_path_to_plasma_file(self, path_to_plasma_file):
        self.d['path_to_plasma_file'] = path_to_plasma_file

    def set_description(self, description):
        self.d['description'] = description

    def set_provider(self, provider):
        self.d['provider'] = provider

    def add_field(self, name, units, precision, description):
        if name in self.d['fields']:
            raise ValueError("A field named '%s' already exists. Use update_field()." % name)

        self.d['fields'][name] = {'name': name, 'units': units, 'precision': precision, 'description': description}
        # self.d['field_order'].append(name)

    def update_field(self, name, units=None, precision=None, description=None):
        if not name in self.d['fields']:
            raise ValueError("A field named '%s' does not exist. Use add_field()." % name)

        if units:
            self.d['fields'][name]['units'] = units

        if precision:
            self.d['fields'][name]['precision'] = precision

        if description:
            self.d['fields'][name]['description'] = description

    def delete_field(self, name):
        if not name in self.d['fields']:
            raise ValueError("A field named '%s' does not exist." % name)

        del(self.d['fields'][name])
        # self.d['field_order'] = [x for x in self.d['field_order'] if x != name]

    def get_fields(self):
        return self.d['fields']

    '''
    def reorder_fields(self, list_of_names):
        for name in list_of_names:
            if not name in self.d['field_order']:
                raise ValueError("A field named '%s' does not exist." % name)

        for name in self.d['field_order']:
            if not name in list_of_names:
                raise ValueError("You must include '%s' in your new field order." % name)

        self.d['field_order'] = list_of_names
    '''

    def json(self):
        return json.dumps(self.d, indent=4)



#   x =


def create_data_source():
    ds = DataSource('phm08-new2')

    ds.set_provider('sample data')
    ds.add_field('sensor01', 'mhz', 2, 'a black box sensor')
    ds.add_field('sensor02', 'mhz', 2, 'a black box sensor')
    ds.add_field('sensor03', 'mhz', 2, 'a black box sensor')
    ds.add_field('sensor04', 'mhz', 2, 'a black box sensor')
    ds.update_field('sensor02', units='khz')
    ds.delete_field('sensor03')



    # ds.reorder_fields([ 'sensor01', 'sensor02', 'sensor04'])


    #ds.delete_field('sensor01')
    #ds.delete_field('sensor02')
    #ds.delete_field('sensor04')


    #print(ds.json())

    return ds



if __name__ == '__main__':
    mongo_client = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    #mongo_client.add_data_source(create_data_source())

    data_sources = mongo_client.get_data_sources()

    for item in data_sources:
        print(item.json())
        mongo_client.remove_data_source(item)

    #data_sources = mongo_client.get_data_sources()

    #for item in data_sources:
    #    print(item.json())

    #mongo_client.add_data_source(ds)

    #mongo_client.delete_data_source(data_sources[1])

    #data_sources = mongo_client.get_data_sources()















#   x = mycol.insert_one(mydict)

