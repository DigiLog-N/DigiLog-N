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
