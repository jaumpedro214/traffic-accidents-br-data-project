import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
import pyspark.sql.functions as F

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

def replace_nulls(df, column, value):
    return (
        df
        .withColumn(
            column,
            F.when(
                F.col(column).isNull() | F.col(column).isin(
                    ["(null)", "NA", "N/A", "null", "na", "n/a"]
                ),
                value
            ).otherwise(
                F.col(column)
            )
        )
    )

def lowercase_column(df, column):
    return df.withColumn(
            column,
            F.lower(F.col(column))
        )

if __name__ == "__main__":

    spark = SparkSession.builder.appName("App").getOrCreate()
    # Reduce loggs to error
    spark.sparkContext.setLogLevel("ERROR")

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

    df_accidents.show(10)
    # DATA INVERSA - DATE
    # formats: yyyy-MM-dd, dd/MM/yyyy and dd/MM/yy

    df_accidents = (
        df_accidents
        .withColumn(
            "DATE",
            F.when(
                F.substring("data_inversa", 4, 1) == "-",
                F.to_date(F.col("data_inversa"), "yyyy-MM-dd")
            ).when(
                F.col("data_inversa").ilike("%/%/__"),
                F.to_date(F.col("data_inversa"), "dd/MM/yy")
            ).otherwise(
                F.to_date(F.col("data_inversa"), "dd/MM/yyyy")
            )
        )
    )

    # Horario - TIME
    # OK

    # km - FLOAT
    # replace: (null) and NA for null
    # replace: , for .

    df_accidents = replace_nulls(df_accidents, "km", F.lit(None))
    df_accidents = (
        df_accidents
        .withColumn(
            "km",
            F.regexp_replace(
                F.col("km"),
                ",", "."
            )
            .cast("float")
        )
    )

    # latitude and longitude - FLOAT
    # replace: null (str) for null 
    # replace: , for .

    df_accidents = replace_nulls(df_accidents, "latitude", F.lit(None))
    df_accidents = replace_nulls(df_accidents, "longitude", F.lit(None))

    df_accidents = (
        df_accidents
        .withColumn(
            "latitude",
            F.regexp_replace(
                F.col("latitude"),
                ",", "."
            )
            .cast("float")
        )
        .withColumn(
            "longitude",
            F.regexp_replace(
                F.col("longitude"),
                ",", "."
            )
            .cast("float")
        )
    )

    # br - STRING
    # replace: (null) and NA for null
    # format with 3 digits
    df_accidents = replace_nulls(df_accidents, "br", F.lit(None))
    df_accidents = (
        df_accidents
        .withColumn(
            "br",
            F.format_string("%03d", F.col("br").cast("int"))
        )
    )
    
    # dia_semana - STRING
    # preprocess: lowercase
    # replace: -feira for nothing

    df_accidents = (
        df_accidents
        .withColumn(
            "dia_semana",
            F.regexp_replace(
                F.lower(
                    F.col("dia_semana")
                ),
                "-feira", ""
            )
        )
    )

    # uf - STRING
    # replace: (null) for null
    df_accidents = replace_nulls(df_accidents, "uf", F.lit(None))

    # causa_acidente - STRING
    # ...

    # tipo_acidente - STRING
    # preproces: lowercase
    # if contains "colisão" and "objeto" -> "colisão com objeto"
    # if contains "queda" -> "queda do veiculo"
    # replace: "Pedestre" = "pessoa"
    # normalize: eventos atípicos, danos eventuais
    # normalize: saída de pista, saída de leito carroçável
    # replace: null (str) for null

    # classificacao_acidente - STRING
    # replace: null and (null) for Ignorado
    df_accidents = replace_nulls(df_accidents, "classificacao_acidente", "Ignorado")

    # fase_dia - STRING
    # preprocess: lowecase
    # replace null and (null) for null
    df_accidents = lowercase_column(df_accidents, "fase_dia")
    df_accidents = replace_nulls(df_accidents, "fase_dia", F.lit(None))

    # condicao_metereologica - STRING
    # preprocess: lowercase
    # normalize: nublado, nevoeiro/neblina
    # normalize: céu claro, ceu claro
    # replace: null and (null) for null
    df_accidents = lowercase_column(df_accidents, "condicao_metereologica")
    df_accidents = replace_nulls(df_accidents, "condicao_metereologica", F.lit(None))
    df_accidents = (
        df_accidents
        .withColumn(
            "condicao_metereologica",
            F.when(
                F.col("condicao_metereologica") == "nevoeiro/neblina",
                "nublado"
            )
            .when(
                F.col("condicao_metereologica") == "ceu claro",
                "céu claro"
            )
        )
    )

    # tipo_pista - STRING
    # replace: (null) for null
    df_accidents = replace_nulls(df_accidents, "tipo_pista", F.lit(None))

    # tracado_via - STRING
    # replace: (null) for null
    # replace: Não Informado for null
    df_accidents = replace_nulls(df_accidents, "tracado_via", "Não Informado")

    # uso_solo - STRING
    # replace: (null) for null
    df_accidents = replace_nulls(df_accidents, "uso_solo", F.lit(None))

    # regional - STRING
    # replace: null, N/A and NA for null
    df_accidents = replace_nulls(df_accidents, "regional", F.lit(None))

    df_accidents.show(100)