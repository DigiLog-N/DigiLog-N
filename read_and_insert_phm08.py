from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry
from PlasmaReader import PlasmaReader
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement


class CassandraAdapter:
    def __init__(self, key_space):
        self.key_space = key_space
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.key_space)

    def __del__(self):
        self.cluster.shutdown()

    def insert_data(self):
        columns = ['unit', 'cycle', 'op1', 'op2', 'op3', 'sensor01', 'sensor02', 'sensor03', 'sensor04', 'sensor05', 'sensor06', 'sensor07', 'sensor08', 'sensor09', 'sensor10', 'sensor11', 'sensor12', 'sensor13', 'sensor14', 'sensor15', 'sensor16', 'sensor17', 'sensor18', 'sensor19', 'sensor20', 'sensor21']

        slot_string = '?,' * len(columns)
        slot_string = slot_string.strip(',')
        prep_string = "INSERT INTO phm08_insert_test(%s) VALUES (%s)" % (','.join(columns), slot_string)
        for field in columns:
            print(column)
            print(fields[column])
            print(results[column])
            print("")

        '''
        insert_sql = self.session.prepare(prep_string)
        batch = BatchStatement()
        batch.add(insert_sql, (1, 'LyubovK', 2555, 'Dubai'))
        batch.add(insert_sql, (2, 'JiriK', 5660, 'Toronto'))
        batch.add(insert_sql, (3, 'IvanH', 2547, 'Mumbai'))
        batch.add(insert_sql, (4, 'YuliaT', 2547, 'Seattle'))
        self.session.execute(batch)
        print('Batch Insert Completed')
        '''


if __name__ == '__main__':
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')
    cassie = CassandraAdapter('digilog_n')

    data_source = dsr.get_data_source('PHM08 Prognostics Data Challenge Dataset')

    if data_source:
        # Readers specific to a particular data-source could be sub-classed from
        # PlasmaReader. Currently, this PlasmaReader rips all columns and stores
        # them as lists in an internal dictionary. You can access all of 'op3'
        # for instance by simply using as below. At some time interval, you can
        # read_all_columns() again, and PlasmaReader will determine which buffers
        # are new, extract their data, mark the buffer names off in its internal
        # list, and append the data to the existing data structure.
        #
        # the user may not want to preserve their own copy of the data in the
        # data structure, hence one could simply delete self.d and reinitialize
        # it to {}, etc. There's a number of possibilities, depending on
        # the usage patterns needed for generating models in pyspark.
        #
        # I also have some code for loading this data into a DataFrame, but I
        # think this might be heavyweight for our use-case.
        pr = PlasmaReader(data_source.get_path_to_plasma_file())
        results = pr.read_all_columns()
        print("There should be 619 results in op3 right now.")
        print(len(results['op3']))

        fields = data_source.get_fields()
        cassie.insert_data()



'''
CREATE TABLE phm08_train(
	//units are not named, only numbered
	unit int,
	//timestamps are not provided, only cycles (presumably take-off + flight + landing = one engine cycle)
	cycle int,
	op1 float,
	op2 float,
	op3 float,
	sensor01 float,
	sensor02 float,
	sensor03 float,
	sensor04 float,
	sensor05 float,
	sensor06 float,
	sensor07 float,
	sensor08 float,
	sensor09 float,
	sensor10 float,
	sensor11 float,
	sensor12 float,
	sensor13 float,
	sensor14 float,
	sensor15 float,
	sensor16 float,
	sensor17 float,
	sensor18 float,
	sensor19 float,
	sensor20 float,
	sensor21 float,
	primary key ((unit), cycle)
); 
'''
