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
from PlasmaWriter import PlasmaWriter





if __name__ == '__main__':
    dsr = DataSourceRegistry('127.0.0.1', 27017, 'digilog_n', 'data_sources')

    data_source = dsr.get_data_source('PHM08 Prognostics Data Challenge Dataset')

    if data_source:
        pr = PlasmaReader(data_source.get_path_to_plasma_file(), 'PHM08')
        pdf = pr.to_pandas()

        spark = SparkSession.builder.getOrCreate()

        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        spark.conf.set("spark.sql.parquet.mergeSchema", "false")
        spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", "false")

        def add_func(a, b):
            return a + b

        add_me = pandas_udf(add_func, returnType=LongType())

        sdf = spark.createDataFrame(pdf)

        result_pdf = sdf.select(add_me(col("op1"), col("op2"))).toPandas()

        pw = PlasmaWriter('/tmp/plasma', 'SPARK_RSLT')
        key = pw.from_pandas(result_pdf)

        print(key)

