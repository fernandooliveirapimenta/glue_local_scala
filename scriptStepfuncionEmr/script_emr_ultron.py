# -*- coding: utf-8 -*-
import sys
from pyspark.context import SparkContext
import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json
import pyarrow.parquet as pq
import pyarrow
import s3fs
import boto3
import re

import time

import io


def mapSchemaColumns(table):
    schema = table.schema
    for i in range(table.num_columns):
        if table.schema[i].type == pyarrow.uint8():        
            schema = schema.set(i, pyarrow.field(table.column_names[i], pyarrow.int32()))
        elif table.schema[i].type == pyarrow.binary():
            schema = schema.set(i, pyarrow.field(table.column_names[i], pyarrow.string()))

    table = table.cast(schema, False)
    del(schema)
    return table
    
def repl(match):
    data = {"á": "a", "ç": "c", "ê": "e", "ã": "a", "í": "i", "é": "e"}
    return data.get(match.group(0))

def renameColumns(table):
    columns_names = table.column_names
    for i in range(len(columns_names)):
        columns_names[i] = re.sub(u'[^a-zA-Z0-9_ ]', repl, columns_names[i])
        columns_names[i] = columns_names[i].replace(' ', '_')
    table = table.rename_columns(columns_names)
    return table

if __name__ == "__main__":

    s3f = s3fs.S3FileSystem()
    s3 = boto3.resource('s3')
    
    spark = SparkSession.builder.appName("spark").getOrCreate()

    sourceBucketName = "dev-brasilseg-ultron-extracao"
    targetBucketName = "dev-brasilseg-data-lake"

    ambiente = "brsegd"

    sourceBucket = s3.Bucket(sourceBucketName)

    files = [file.key for file in list(sourceBucket.objects.filter(Prefix="{}_extracao/".format(ambiente))) if not file.key[-1:] == '/']


    for file in files:

        base = file.split("/")[1]
        tabela = file.split("/")[3]

        fileName = file.split("/")[4]

        data = io.BytesIO()

        sourceBucket.download_fileobj(file, data)
        #sourceBucket.download_file(file, fileName)


        table = pq.read_table(data)

        del(data)

        table = mapSchemaColumns(table)
        table = renameColumns(table)

        pq.write_table(table, "s3://{}/{}".format(sourceBucketName, file), filesystem=s3f)

        #os.remove(fileName)

        time.sleep(5)

        df = spark.read.parquet("s3://{}/{}".format(sourceBucketName, file))

        #Remover a coluna binary
        if "lock" in df.columns:
            df = df.drop("lock")

        if 'Op' not in df.columns:
            df = df.withColumn('Op', lit("I"))

        if "dt_inclusao" in df.columns:
            df = df.withColumn('Ano', year(to_date(col('dt_inclusao'), 'yyyy-MM-dd HH:mm:ss')).cast("int"))
            df = df.withColumn('Mes', month(to_date(col('dt_inclusao'), 'yyyy-MM-dd HH:mm:ss')).cast("int"))
        else:
            df = df.withColumn('Ano', year(to_date(col('data_exportacao'), 'yyyy-MM-dd HH:mm:ss')).cast("int"))
            df = df.withColumn('Mes', month(to_date(col('data_exportacao'), 'yyyy-MM-dd HH:mm:ss')).cast("int"))

        df.write.partitionBy("Ano", "Mes") \
            .mode("append") \
            .parquet("s3://{}/{}/{}/{}".format(targetBucketName, "ultron", base, tabela.lower()))

        parquetBucket = { "Bucket": sourceBucketName, "Key" : file}
        #parquetTarget = file.replace("{}_extracao_parquet".format(ambiente), "{}_processados_parquet".format(ambiente))

        s3.meta.client.copy(parquetBucket, sourceBucketName, file.replace("extracao", "processados"))
        obj = s3.Object(sourceBucketName, file)
        obj.delete()
        #sourceBucket.copy(parquetBucket, parquetTarget)

        del(df)
        del(table)
        del(parquetBucket)