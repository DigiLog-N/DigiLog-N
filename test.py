import pymongo
import json
from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry


def create_data_source():
    ds = DataSource('phm08-new2')

    ds.set_provider('sample data')
    ds.add_field('sensor01', 'mhz', 2, 'a black box sensor')
    ds.add_field('sensor02', 'mhz', 2, 'a black box sensor')
    ds.add_field('sensor03', 'mhz', 2, 'a black box sensor')
    ds.add_field('sensor04', 'mhz', 2, 'a black box sensor')
    ds.update_field('sensor02', units='khz')
    ds.delete_field('sensor04')

    return ds


if __name__ == '__main__':
    mongo_client = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    #mongo_client.add_data_source(create_data_source())

    data_sources = mongo_client.get_data_sources()

    for item in data_sources:
        print(item.json())
        #mongo_client.remove_data_source(item)

    #exit(1)
    ds = data_sources[0]
    ds.add_field('new_field6', 'some_unit', 4, 'sample value')

    foo = mongo_client.update_data_source(ds)

    print("hi")

    #data_sources = mongo_client.get_data_sources()

    #for item in data_sources:
    #    print(item.json())

    #mongo_client.add_data_source(ds)

    #mongo_client.delete_data_source(data_sources[1])

    #data_sources = mongo_client.get_data_sources()









