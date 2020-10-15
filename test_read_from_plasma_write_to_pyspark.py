import pyarrow as pa
import pandas as pd
import pyspark
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
from pyspark.sql import SparkSession
from PlasmaReader import PlasmaReader
from DataSource import DataSource
from DataSourceRegistry import DataSourceRegistry
from PlasmaReader import PlasmaReader


def read_test(plasma_file_path):
    # Use our PlasmaReader object to read all the data currently in Plasma and
    # convert it into a Pandas DataFrame.
    pdf_list = []

    pr = PlasmaReader(plasma_file_path)
    pdf = pr.to_pandas()
    print(pdf.info())
    print(pdf.shape)
    pdf.to_csv('./test.csv')
    pdf_list.append(pdf)

    # Use it again to see if there's any new data. Since this is loaded from
    # a fixed file, there won't be.
    pdf = pr.to_pandas()
    if pdf:
        print(pdf.info())
        print(pdf.shape)
        pdf.to_csv('./test2.csv')
        pdf_list.append(pdf)
    else:
        print("no new data!")

    # return the list of Pandas DataFrames to be used in a possible write-test.
    return pdf_list


def write_test(pandas_data_frame):
    spark = SparkSession.builder.getOrCreate()

    # tests show this option really does improve times:
    # from nearly 5 minutes down to 20 seconds for 10,000,000 rows.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # for now, throw an error, rather than fallback to the non-arrow option.
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    # these I don't know much about, but it was recommended.
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", "false")

    '''
    # Declare the function and create the UDF
    def multiply_func(a, b):
        return a * b

    multiply = pandas_udf(multiply_func, returnType=LongType())
    l = []
    for i in range(0, 10000000):
        l.append(i)
    x = pd.Series(l)

    print(multiply_func(x, x))

    df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))
    df.select(multiply(col("x"), col("x"))).show()
    '''
    def add_func(a, b):
        return a + b

    add_me = pandas_udf(add_func, returnType=LongType())
    sdf = spark.createDataFrame(pandas_data_frame)
    sdf.select(add_me(col("op1"), col("op2"))).show()


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
        l = read_test(data_source.get_path_to_plasma_file())
        write_test(l[0])
