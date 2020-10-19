from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry
from PlasmaReader import PlasmaReader


if __name__ == '__main__':
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

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
        for field in fields:
            print(field)
            print(fields[field])
            print(results[field])
            print("")
