from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip


def replace_nulls(df, column, value):
    """Replace some pedefined strings and NULLs in the column with the specified value

    Args:
        df (spark.sql.DataFrame): Spark dataframe
        column (str): The column, must be of type str
        value (str): The value used to replace 

    Returns:
        spark.sql.DataFrame: Dataframe with the values replaced
    """
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
    """Lowercase an entire column

    Args:
        df (spark.sql.DataFrame): Spark Dataframe
        column (str): The column 

    Returns:
        spark.sql.DataFrame: the dataframe with the column lowercased
    """
    return df.withColumn(
            column,
            F.lower(F.col(column))
        )

if __name__ == "__main__":

    # Instantiate and configure the Spark Session with delta lake
    builder = SparkSession.builder.appName("ConvertRawToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Reduce loggs to error
    spark.sparkContext.setLogLevel("ERROR")
    # Reduce the number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", 4)

    # Read the data
    df_accidents = spark\
        .read.format("delta")\
        .load("/data/accidents_raw_union/")\
        .select(
            [
                'id', 'data_inversa',
                'dia_semana', 'horario', 
                'uf', 'br', 'km', 'municipio', 
                'causa_acidente', 'tipo_acidente', 
                'classificacao_acidente', 'fase_dia', 
                'sentido_via', 'condicao_metereologica', 
                'tipo_pista', 'tracado_via', 
                'uso_solo', 'pessoas', 
                'mortos', 'feridos_leves', 
                'feridos_graves', 'ilesos', 
                'ignorados', 'feridos', 'veiculos', 
                'latitude', 'longitude'
            ]
        )


    # DATA INVERSA - DATE
    # formats: yyyy-MM-dd, dd/MM/yyyy and dd/MM/yy

    df_accidents = (
        df_accidents
        .withColumn(
            "data_inversa",
            F.when(
                F.col("data_inversa").ilike("____-%"),
                F.to_date(F.col("data_inversa"), "yyyy-MM-dd")
            ).when(
                F.col("data_inversa").ilike("%/%/__"),
                F.to_date(F.col("data_inversa"), "dd/MM/yy")
            ).otherwise(
                F.to_date(F.col("data_inversa"), "dd/MM/yyyy")
            )
        )
        .withColumnRenamed("data_inversa", "date")
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
    df_accidents = lowercase_column(df_accidents, "causa_acidente")
    df_accidents = replace_nulls(df_accidents, "causa_acidente", F.lit(None))
    replace_by = [
        (F.lit(None), "outras"),
        ("Falta de aten????o", ["Falta de aten????o","Falta de Aten????o ?? Condu????o"]),
        ("Ingest??o de ??lcool ou psicoativos", [
                "Ingest??o de subst??ncias psicoativas pelo condutor", 
                "Ingest??o de ??lcool pelo condutor", "Ingest??o de ??lcool", 
                "Ingest??o de Subst??ncias Psicoativas", "Ingest??o de ??lcool"
        ]),
        ("N??o guardar dist??ncia de seguran??a",[
            "N??o guardar dist??ncia de seguran??a", 
            "Condutor deixou de manter dist??ncia do ve??culo da frente"
        ]),
        ("Defeito Mec??nico no Ve??culo",[
            "Problema com o freio", "Problema na suspens??o", 
            "Defeito mec??nico em ve??culo", 
            "Avarias e/ou desgaste excessivo no pneu", 
            "Defeito Mec??nico no Ve??culo", 
            "Demais falhas mec??nicas ou el??tricas", "Far??is desregulados",
            "Defici??ncia ou n??o Acionamento do Sistema de Ilumina????o/Sinaliza????o do Ve??culo"
        ]),
        ("Defeito na via",[
            "Demais falhas na via", 
            "Defeito na via", "Defeito na Via", 
            "Pista esburacada", 
            "Falta de acostamento",
            "Afundamento ou ondula????o no pavimento",
            "Falta de elemento de conten????o que evite a sa??da do leito carro????vel"
        ]),
        ("Condutor dormindo",[
            "Dormindo", "Condutor Dormindo"
        ]),
        ("Desobedi??ncia ?? sinaliza????o",[
            "Desobedi??ncia ?? sinaliza????o", 
            "Condutor desrespeitou a ilumina????o vermelha do sem??foro"
        ]),
        ("Desobedi??ncia ??s normas de tr??nsito pelo condutor",[
            "Desobedi??ncia ??s normas de tr??nsito pelo condutor", 
            "Desrespeitar a prefer??ncia no cruzamento",
        ]),
        ("Mal S??bito",[
            "Mal S??bito", "Mal s??bito do condutor"
        ]),
        ("Rea????o tardia ou ineficiente do condutor",[
            "Rea????o tardia ou ineficiente do condutor", 
            "Aus??ncia de rea????o do condutor"
        ]),
        ("Fen??menos da Natureza",[
            "Fen??menos da Natureza",
            "Demais Fen??menos da natureza",
            "Chuva", "Neblina", "Fuma??a"
        ]),
        ("Desobedi??ncia ??s normas de tr??nsito pelo pedestre",[
            "Pedestre andava na pista", 
            "Entrada inopinada do pedestre", 
            "Desobedi??ncia ??s normas de tr??nsito pelo pedestre", 
            "Pedestre cruzava a pista fora da faixa"
        ]),
        ("Ingest??o de ??lcool ou psicoativos pelo pedestre",[
            "Ingest??o de ??lcool ou de subst??ncias psicoativas pelo pedestre", 
            "Ingest??o de ??lcool e/ou subst??ncias psicoativas pelo pedestre"
        ]),
        ("Transitar no acostamento",[
            "Transitar no acostamento", 
            "Transitar na cal??ada"
        ]),
        ("Acumulo de material sobre o pavimento",[
            "Acumulo de areia ou detritos sobre o pavimento", 
            "Acumulo de ??leo sobre o pavimento", 
            "Acumulo de ??gua sobre o pavimento"
        ])
    ]

    replace_causa_acidente_rule = F.when(
        F.col("causa_acidente").isin( replace_by[0][1] ), replace_by[0][0]
    )
    for replace_value, values_to_replace in replace_by[1:]:
        values_to_replace = [ value.lower() for value in values_to_replace ]
        replace_causa_acidente_rule = replace_causa_acidente_rule.when(
            F.col("causa_acidente").isin( values_to_replace ),
            replace_value
        )
    replace_causa_acidente_rule = replace_causa_acidente_rule.otherwise(F.col("causa_acidente"))

    df_accidents = (
        df_accidents
        .withColumn(
            "causa_acidente",
            replace_causa_acidente_rule
        )
    )

    # tipo_acidente - STRING
    # preproces: lowercase
    # if contains "colis??o" and "objeto" -> "colis??o com objeto"
    # if contains "queda" -> "queda do veiculo"
    # replace: "Pedestre" = "pessoa"
    # normalize: sa??da de pista, sa??da de leito carro????vel
    # replace: null (str) for null
    df_accidents = lowercase_column(df_accidents, "tipo_acidente")
    df_accidents = replace_nulls(df_accidents, "tipo_acidente", F.lit(None))
    df_accidents = (
        df_accidents
        .withColumn(
            "tipo_acidente",
            F
            .when(
                F.col("tipo_acidente").contains("colis??o")
                & F.col("tipo_acidente").contains("objeto"),
                "colis??o com objeto"
            )
            .when(
                F.col("tipo_acidente").contains("queda"),
                "queda do veiculo"
            )
            .when(
                F.col("tipo_acidente").contains("pedestre"),
                F.regexp_replace( F.col("tipo_acidente"), "pedestre", "pessoa" )
            )
            .when(
                F.col("tipo_acidente") == "sa??da de leito carro????vel",
                "sa??da de pista"
            )
            .otherwise(F.col("tipo_acidente"))
        )
    )

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
    # normalize: c??u claro, ceu claro
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
                "c??u claro"
            )
        )
    )

    # tipo_pista - STRING
    # replace: (null) for null
    df_accidents = replace_nulls(df_accidents, "tipo_pista", F.lit(None))

    # tracado_via - STRING
    # replace: (null) for null
    # replace: N??o Informado for null
    df_accidents = replace_nulls(df_accidents, "tracado_via", "N??o Informado")

    # uso_solo - STRING
    # replace: (null) for null
    df_accidents = replace_nulls(df_accidents, "uso_solo", F.lit(None))
    
    # Columns with counts
    # pessoas, mortos, feridos_leves, feridos_graves, ilesos, feridos, ignorados, veiculos - INT
    # replace: null, N/A and NA for null

    count_columns = [
        "pessoas", "mortos", "feridos_leves", 
        "feridos_graves", "ilesos", "feridos", 
        "ignorados", "veiculos"
    ]

    for column in count_columns:
        df_accidents = replace_nulls(df_accidents, column, F.lit(None))
        df_accidents = (
            df_accidents
            .withColumn(
                column,
                F.col(column).cast("int")
            )
        )


    # Save to Parquet
    df_accidents\
        .write.format("parquet")\
        .mode("overwrite")\
        .save("/data/accidents_silver.parquet")
    
    df_accidents.printSchema()
    df_accidents.show(3)