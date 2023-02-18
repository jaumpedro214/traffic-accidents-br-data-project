from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":

    # Instantiate and configure the Spark Session with delta lake
    builder = SparkSession.builder.appName("CountNumAccidentsYearGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Reduce loggs to error
    spark.sparkContext.setLogLevel("ERROR")
    # Reduce the number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    df_accidents_gold = spark\
        .read.format("parquet")\
        .load("/data/accidents_gold_agg.parquet")
    
    df_accidents_gold\
        .withColumn("ANO", F.year(F.col("DATA")))\
        .groupBy("ANO")\
        .agg(
            F.sum(F.col("QT_ACIDENTES")).alias("QT_ACIDENTES_ANO"),
            F.sum(F.col("QT_TOTAL_PESSOAS")).alias("QT_PESSOAS"),
            F.sum(F.col("QT_TOTAL_MORTOS")).alias("QT_MORTOS"),
        )\
        .orderBy("ANO")\
        .show()