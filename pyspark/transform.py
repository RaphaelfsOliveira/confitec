from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, col, to_timestamp, udf
from pyspark.sql.types import *
from datetime import datetime


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet('OriginaisNetflix.parquet')


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


    df2 = df.dropDuplicates()

    df2 = df2.withColumn('Premiere', premiere_to_datetime(df2.Premiere)) \
            .withColumn('dt_inclusao', dt_inclusao_to_datetime(df2.dt_inclusao)) \
            .withColumn('Seasons', change_name(df2.Seasons)) \
            .withColumn('Data de Alteração', set_timestamp())
    
    df2.createOrReplaceTempView("Netflix")
    
    query_order_by = "SELECT * from Netflix ORDER BY Active DESC, Genre DESC"
    df2 = spark.sql(query_order_by)

    df2.show(10, False)
    
    # df2.printSchema()

    


    # df.toDF(
    #     'Título',
    #     'Gênero',
    #     'Rótulos de gênero',
    #     'Estreia',
    #     'Temporadas',
    #     'Temporadas analisadas',
    #     'Episódios analisados',
    #     'Comprimento',
    #     'Comprimento mínimo',
    #     'Comprimento máximo',
    #     'Status',
    #     'Ativo',
    #     'Tabela',
    #     'Idioma',
    #     'Data Inclusão'
    # ).show(5, False)
