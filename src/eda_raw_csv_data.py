import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("App").getOrCreate()

# Reduce loggs to error
spark.sparkContext.setLogLevel("ERROR")

SCHEMA = StructType(
    [
        StructField('id', StringType(), True), StructField('data_inversa', StringType(), True),
        StructField('dia_semana', StringType(), True), StructField('horario', StringType(), True), 
        StructField('uf', StringType(), True), StructField('br', StringType(), True), 
        StructField('km', StringType(), True), StructField('municipio', StringType(), True), 
        StructField('causa_acidente', StringType(), True), StructField('tipo_acidente', StringType(), True), 
        StructField('classificacao_acidente', StringType(), True), StructField('fase_dia', StringType(), True), 
        StructField('sentido_via', StringType(), True), StructField('condicao_metereologica', StringType(), True), 
        StructField('tipo_pista', StringType(), True), StructField('tracado_via', StringType(), True), 
        StructField('uso_solo', StringType(), True), StructField('pessoas', StringType(), True), 
        StructField('mortos', StringType(), True), StructField('feridos_leves', StringType(), True), 
        StructField('feridos_graves', StringType(), True), StructField('ilesos', StringType(), True), 
        StructField('ignorados', StringType(), True), StructField('feridos', StringType(), True), 
        StructField('veiculos', StringType(), True), StructField('latitude', StringType(), True), 
        StructField('longitude', StringType(), True), StructField('regional', StringType(), True), 
        StructField('delegacia', StringType(), True), StructField('uop', StringType(), True)
    ]
)

def list_all_number_formats(df, column):

    print( 
        f"List all number formats from column {column}:\n\n"
    )

    (
        df
        .select(column)
        # Replace all numbers with D
        .withColumn(column, F.regexp_replace(column, r"\d", "D"))
        .distinct()
        .show( 200, False )
    )

def list_all_unique_values_from_column(df, column):

    print( 
        f"List all unique values from column {column}:\n\n"
    )

    (
        df
        .select(column)
        .distinct()
        .show( 200, False )
    )



if __name__ == "__main__":
    BASE_PATH = "/data/acidentes"
    files = [f for f in os.listdir(BASE_PATH) if f.endswith('.csv')]

    df_accidents = (
        spark
        .read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("encoding","ISO-8859-1")
        .schema(SCHEMA)
        .load(f"{BASE_PATH}/*.csv")
    )

    list_all_number_formats(df_accidents, "data_inversa")
    list_all_number_formats(df_accidents, "horario")
    list_all_number_formats(df_accidents, "km")
    list_all_number_formats(df_accidents, "latitude")
    list_all_number_formats(df_accidents, "longitude")
    list_all_number_formats(df_accidents, "br")

    list_all_unique_values_from_column(df_accidents, "dia_semana")
    list_all_unique_values_from_column(df_accidents, "uf")
    list_all_unique_values_from_column(df_accidents, "causa_acidente")
    list_all_unique_values_from_column(df_accidents, "tipo_acidente")
    list_all_unique_values_from_column(df_accidents, "classificacao_acidente")
    list_all_unique_values_from_column(df_accidents, "fase_dia")
    list_all_unique_values_from_column(df_accidents, "sentido_via")
    list_all_unique_values_from_column(df_accidents, "condicao_metereologica")
    list_all_unique_values_from_column(df_accidents, "tipo_pista")
    list_all_unique_values_from_column(df_accidents, "tracado_via")
    list_all_unique_values_from_column(df_accidents, "uso_solo")
    list_all_unique_values_from_column(df_accidents, "regional")