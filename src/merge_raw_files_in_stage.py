import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    # This script merges all files from all years in a single delta table
    # spark-submit --packages io.delta:delta-core_2.12:2.1.0 merge_raw_files_in_stage.py

    # Instantiate and configure the Spark Session with delta lake
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Reduce loggs to error
    spark.sparkContext.setLogLevel("ERROR")
    # Reduce the number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    BASE_PATH = "/data/acidentes"
    files = [f for f in os.listdir(BASE_PATH) if f.endswith('.csv')]
    files.sort()

    # Write the 1st file to a delta table
    # and iteratively evolve the schema using the subsequent tables
    
    spark\
        .read\
        .format("csv")\
        .option("delimiter", ";")\
        .option("encoding", "ISO-8859-1")\
        .option("header", "true")\
        .load(f"{BASE_PATH}/{files[0]}")\
        .withColumn("source_filename", F.input_file_name())\
        .write\
        .partitionBy(["source_filename"])\
        .format("delta")\
        .save("/data/accidents_raw_union")
    
    for file in files:

        print( f"Writing file {file} to the delta table" )
        spark\
            .read\
            .format("csv")\
            .option("delimiter", ";")\
            .option("encoding", "ISO-8859-1")\
            .option("header", "true")\
            .load(f"{BASE_PATH}/{file}")\
            .withColumn("source_filename", F.input_file_name())\
            .write\
            .format("delta")\
            .mode("append")\
            .option("mergeSchema", "true")\
            .save("/data/accidents_raw_union")
