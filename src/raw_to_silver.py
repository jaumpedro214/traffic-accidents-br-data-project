import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

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

    df_accidents.show(10)
    # DATA INVERSA - DATE
    # formats: yyyy-MM-dd, dd/MM/yyyy and dd/MM/yy

    # Horario - TIME
    # OK

    # km - FLOAT
    # replace: (null) and NA for null
    # replace: , for .

    # latitude and longitude - FLOAT
    # replace: null (str) for null 
    # replace: , for .

    # br - STRING
    # replace: (null) and NA for null
    
    # dia_semana - STRING
    # preprocess: lowercase
    # replace: -feira for nothing

    # uf - STRING
    # replace: (null) for null

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

    # fase_dia - STRING
    # preprocess: lowecase
    # replace null and (null) for null

    # condicao_metereologica - STRING
    # preprocess: lowercase
    # normalize: nublado, nevoeiro/neblina
    # normalize: céu claro, ceu claro
    # replace: null and (null) for null

    # tipo_pista - STRING
    # replace: (null) for null

    # tracado_via - STRING
    # replace: (null) for null
    # replace: Não Informado for null

    # uso_solo - STRING
    # replace: (null) for null

    # regional - STRING
    # replace: null, N/A and NA for null