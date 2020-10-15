import pyarrow as pa
import pandas as pd
import pyspark
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
from pyspark.sql import SparkSession
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


l = read_test('/tmp/plasma')
write_test(l[0])
