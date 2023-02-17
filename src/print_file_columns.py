import os
from pyspark.sql import SparkSession

BASE_PATH = "/data/acidentes"

# list all .csv

files = [f for f in os.listdir(BASE_PATH) if f.endswith('.csv')]
files.sort()

spark = SparkSession.builder.appName("App").getOrCreate()

# Reduce loggs to error
spark.sparkContext.setLogLevel("ERROR")

columns = set()

for file in files:
    df = (
        spark
        .read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("encoding","ISO-8859-1")
        .load("{}/{}".format(BASE_PATH, file))
        .limit(30)
    )

    if len(columns) == 0:
        columns = set(df.columns)
    columns = columns.intersection(set(df.columns))

    print(file)
    df.printSchema()
    print("\n")

    print(f"Common columns: {list(columns)}")
