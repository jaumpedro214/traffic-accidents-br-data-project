import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    # spark-submit --packages io.delta:delta-core_2.12:2.1.0 eda_raw_delta_data.py

    # Instantiate and configure the Spark Session with delta lake
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Reduce loggs to error
    spark.sparkContext.setLogLevel("ERROR")
    # Reduce the number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    df = spark\
        .read.format("delta")\
        .load("/data/accidents_raw_union/")\
        .sample(0.05, 214)\
        .show(200)