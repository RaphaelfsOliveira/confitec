from pyspark.sql import SparkSession
from pyspark.sql.functions import  udf
from pyspark.sql.types import *
from datetime import datetime
import os


@udf(returnType=TimestampType())
def premiere_to_datetime(date):
    date = date.replace('-', ' ')
    return datetime.strptime(date, '%d %b %y')


@udf(returnType=TimestampType())
def dt_inclusao_to_datetime(date):
    date = date.split('.')[0]
    return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')


@udf(returnType=StringType())
def change_name(text):
    old_type = 'tba'
    new_type = "a ser anunciado"
    
    if text.lower() == old_type:
        return new_type
    return text


@udf(returnType=TimestampType())
def set_timestamp():
    return datetime.now()


def read_file(file_name):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(file_name)
    return df, spark


def transform_file(df, spark):
    df2 = df.dropDuplicates()

    df2 = df2.withColumn('Premiere', premiere_to_datetime(df2.Premiere)) \
             .withColumn('dt_inclusao', dt_inclusao_to_datetime(df2.dt_inclusao)) \
             .withColumn('Seasons', change_name(df2.Seasons)) \
             .withColumn('Data de Alteração', set_timestamp())
    
    df2.createOrReplaceTempView("Netflix")
    
    query_order_by = "SELECT * from Netflix ORDER BY Active DESC, Genre DESC"
    df2 = spark.sql(query_order_by)

    df2 = df2.toDF(
        'Título',
        'Gênero',
        'Rótulos de gênero',
        'Estreia',
        'Temporadas',
        'Temporadas analisadas',
        'Episódios analisados',
        'Comprimento',
        'Comprimento mínimo',
        'Comprimento máximo',
        'Status',
        'Ativo',
        'Tabela',
        'Idioma',
        'Data Inclusão',
        'Data de Alteração'
    )

    return df2


def write_csv(data_frame, file_name, sep):
    file_path = os.path.join(file_name)
    data_frame.repartition(1).write.csv(file_path, sep=sep, header=True)
    return True


if __name__ == '__main__':
    source_file = 'OriginaisNetflix.parquet'
    destiny_file = 'originais_netflix'
    
    df, spark = read_file(source_file)

    df2 = transform_file(df, spark)

    write_csv(data_frame=df2, file_name=destiny_file, sep=';')

    print('end')