import pymongo
import socket
import sys
import json
import re
from time import sleep
from WeatherDataSource import WeatherDataSource
from DataSourceRegistry import DataSourceRegistry
import os


def read_csv_file(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        lines = [x.strip() for x in lines]
        d = {'header':{}}
        d['header']['stid'] = lines.pop(0).replace('# STATION: ', '').strip()
        d['header']['name'] = lines.pop(0).replace('# STATION NAME: ', '').strip()
        d['header']['lat'] = float(lines.pop(0).replace('# LATITUDE: ', '').strip())
        d['header']['lon'] = float(lines.pop(0).replace('# LONGITUDE: ', '').strip())
        d['header']['elev'] = float(lines.pop(0).replace('# ELEVATION [ft]: ', '').strip())
        d['header']['state'] = lines.pop(0).replace('# STATE: ', '').strip()
        d['header']['column_names'] = lines.pop(0).split(',')
        d['header']['column_units'] = lines.pop(0).split(',')

        # swap timestamp and station per John's request
        a = d['header']['column_names'][0]
        d['header']['column_names'][0] = d['header']['column_names'][1]
        d['header']['column_names'][1] = a

        lines = [x.split(',') for x in lines]

        d['lines'] = []

        for line in lines:
            a = line[0]
            line[0] = line[1]
            line[1] = a

            # sending it as a csv line means that it must stay string.
            #for i in range(0, len(line)):
            #    if re.match(r'^\d+$', line[i]) is not None:
            #        line[i] = int(line[i])
            #    elif re.match(r'^\d+\.\d+$', line[i]) is not None:
            #        line[i] = float(line[i])

            d['lines'].append(','.join(line))

        return d


def map_source(header):
    ds = WeatherDataSource('SynopticData Meteorology Station %s' % header['stid'])
    ds.set_description(header['name'])
    ds.set_provider('SynopticData + 3rd Parties')
    ds.set_latitude(header['lat'])
    ds.set_longitude(header['lon'])
    ds.set_elevation(header['elev'])
    ds.set_state(header['state'])
   
    count = len(header['column_names'])
    for i in range(0, count):
        column_name = header['column_names'][i]
        column_units = header['column_units'][i]
        try:
            ds.add_field(column_name, column_units, 0, None, 'float')
        except ValueError as e:
            # in this case, we know there's one station with a duplicate column by name and by value
            pass

    return ds


if __name__ == '__main__':
    mongo_client = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    for root, dirs, files in os.walk(sys.argv[1]):
        for f in files:
            file_path = '%s/%s' % (root, f)
            data = read_csv_file(file_path)
            header = data['header']
            ds = map_source(header)
            mongo_client.add_data_source(ds)

    data_sources = mongo_client.get_data_sources()

    for item in data_sources:
        print(item.json())
        if 'SynopticData' in item.get_name():
            #mongo_client.remove_data_source(item)
            pass
