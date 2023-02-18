from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
import os

if __name__ == "__main__":
    #spark-submit --packages io.delta:delta-core_2.12:2.1.0 silver_to_gold.py
    
    # Instantiate and configure the Spark Session with delta lake
    builder = SparkSession.builder.appName("ConvertSilverToGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Reduce loggs to error
    spark.sparkContext.setLogLevel("ERROR")
    # Reduce the number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    # GET THE CREDENTIALS FOR THE GCP BUCKET
    gcp_credentials_path = "/src/credentials/"
    # Find JSON in the path
    for file in os.listdir(gcp_credentials_path):
        if file.endswith(".json"):
            gcp_credentials_path = gcp_credentials_path + file
            break

    # Read the txt in path and read it to the variable BUCKET_NAME
    # the txt file must contain only one line with the bucket name
    gcp_bucket_name = ""
    with open("/src/credentials/bucket_name.txt", "r") as f:
        gcp_bucket_name = f.read()

    # Insert JSON GCP credentials in the spark session
    spark._jsc.hadoopConfiguration().set(
        "google.cloud.auth.service.account.json.keyfile",
        gcp_credentials_path
    )

    df_accidents_silver = spark\
        .read.format("parquet")\
        .load("/data/accidents_silver.parquet")
    
    df_accidents_silver.show(10)
    df_accidents_silver.printSchema()

    df_accidents_silver = (
        df_accidents_silver
        .groupBy([
            # Weather conditions
            "date", "dia_semana", "fase_dia", "condicao_metereologica",
            # State and Highway
            "uf", "br",
            # Cause, type
            "causa_acidente", "tipo_acidente",
        ])
        .agg(
            F.count("*").alias("QT_ACIDENTES"),

            F.sum(
                F.when(
                    F.col("mortos").isNotNull() & (F.col("mortos")>0),
                    1
                ).otherwise(0)
            ).alias("QT_ACIDENTES_COM_MORTOS"),

            F.sum(
                F.when(
                    F.col("feridos").isNotNull() & (F.col("feridos")>0),
                    1
                ).otherwise(0)
            ).alias("QT_ACIDENTES_COM_FERIDOS"),

            F.sum(
                F.when(
                    F.col("feridos_leves").isNotNull() & (F.col("feridos_leves")>0),
                    1
                ).otherwise(0)
            ).alias("QT_ACIDENTES_COM_FERIDOS_LEVES"),

            F.sum(
                F.when(
                    F.col("feridos_graves").isNotNull() & (F.col("feridos_graves")>0),
                    1
                ).otherwise(0)
            ).alias("QT_ACIDENTES_COM_FERIDOS_GRAVES"),

            F.sum("pessoas").alias("QT_TOTAL_PESSOAS"),
            F.sum("ilesos").alias("QT_TOTAL_ILESOS"),
            F.sum("feridos").alias("QT_TOTAL_FERIDOS"),
            F.sum("feridos_leves").alias("QT_TOTAL_FERIDOS_LEVES"),
            F.sum("feridos_graves").alias("QT_TOTAL_FERIDOS_GRAVES"),
            F.sum("mortos").alias("QT_TOTAL_MORTOS"),
            F.sum("ignorados").alias("QT_TOTAL_IGNORADOS"),
        )
        .withColumnRenamed("date", "DATA")
        .withColumnRenamed("dia_semana", "DS_DIA_SEMANA")
        .withColumnRenamed("fase_dia", "DS_FASE_DIA")
        .withColumnRenamed("condicao_metereologica", "DS_CONDICAO_METEREOLOGICA")
        .withColumnRenamed("uf", "SG_UF")
        .withColumnRenamed("br", "DS_BR")
        .withColumnRenamed("causa_acidente", "DS_CAUSA")
        .withColumnRenamed("tipo_acidente", "DS_TIPO")
    )
    
    # Write to GCP bucket
    df_accidents_silver\
        .write.format("parquet")\
        .mode("overwrite")\
        .option("path", f"gs://{gcp_bucket_name}/data/accidents_gold_agg.parquet/")\
        .save()
    
    # Save locally
    # df_accidents_silver\
    #     .write.format("parquet")\
    #     .mode("overwrite")\
    #     .save("/data/accidents_gold_agg.parquet/")

    df_accidents_silver.show(10)