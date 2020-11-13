
#bb30_carga_assistencia_redis_ultron_historico
#bb30_carga_assistencia_redis_ultron_incremental

# dev
aws glue update-job \
  --job-name bb30_carga_assistencia_redis_ultron_historico \
  --output json \
  --job-update '{
  "Role": "arn:aws:iam::533865883733:role/Brasilseg-GlueRole",
  "Command": {
    "Name": "glueetl",
    "ScriptLocation": "s3://datalake-glue-scripts-dev/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala"
  },
  "DefaultArguments": {
    "--job-language": "scala",
    "--extra-jars": "s3://datalake-glue-scripts-dev/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts-dev/dependencias_scala/Assistencia_ABS/entityDependencies.jar",
    "--extra-files": "s3://datalake-glue-scripts-dev/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts-dev/estrutura_query/AssistenciaUltronQuerys.json",
    "--class": "GlueApp",
    "--TempDir": "s3://aws-glue-temporary-533865883733-us-east-1/admin",
    "--conf": "spark.driver.memory=10g",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-s3-parquet-optimized-committer": "true",
    "--enable-spark-ui": "true",
    "--job-bookmark-option": "job-bookmark-disable",
    "--enable-metrics": "true",
    "--environment": "aws",
    "--redis_db": "2",
    "--database": "ultron",
    "--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint",
    "--redis_port": "/brseg/elasticache/brasilseg-redis/porta",
    "--redis_auth": "/brseg/elasticache/brasilseg-redis/token",
    "--redis_ssl": "true",
    "--filter": "%CARGA_BB_3.0%",
    "--redshift_credentials": "dev/redshiftDW",
    "--redshift_schema": "spectrum_assistencia_ultron",
    "--schema": "estrutura_assistencia_ultron.json",
    "--redshift_key_path": "s3://dev-brasilseg-segbr-extracao/chaves-redis/ultron/assistencia",
    "--queries": "AssistenciaUltronQuerys.json",
    "--spark-event-logs-path": "s3://dev-brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_historico/",
    "--type": "full"
  },
  "Connections": {
    "Connections": [
      "Redshift_DataLake"
    ]
  },
  "WorkerType": "Standard",
  "NumberOfWorkers": 5,
  "GlueVersion": "2.0"
}' \
  --region us-east-1 \
  --profile dev


aws glue update-job \
  --job-name bb30_carga_assistencia_redis_ultron_incremental \
  --output json \
  --job-update '{
  "Role": "arn:aws:iam::533865883733:role/Brasilseg-GlueRole",
  "Command": {
    "Name": "glueetl",
    "ScriptLocation": "s3://datalake-glue-scripts-dev/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala"
  },
  "DefaultArguments": {
    "--job-language": "scala",
    "--extra-jars": "s3://datalake-glue-scripts-dev/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts-dev/dependencias_scala/Assistencia_ABS/entityDependencies.jar",
    "--extra-files": "s3://datalake-glue-scripts-dev/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts-dev/estrutura_query/AssistenciaUltronQuerys.json",
    "--class": "GlueApp",
    "--TempDir": "s3://aws-glue-temporary-533865883733-us-east-1/admin",
    "--conf": "spark.driver.memory=10g",
    "--enable-continuous-cloudwatch-log": "true",
    "--enable-s3-parquet-optimized-committer": "true",
    "--enable-spark-ui": "true",
    "--job-bookmark-option": "job-bookmark-disable",
    "--enable-metrics": "true",
    "--environment": "aws",
    "--redis_db": "2",
    "--database": "ultron",
    "--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint",
    "--redis_port": "/brseg/elasticache/brasilseg-redis/porta",
    "--redis_auth": "/brseg/elasticache/brasilseg-redis/token",
    "--redis_ssl": "true",
    "--filter": "%CARGA_BB_3.0%",
    "--redshift_credentials": "dev/redshiftDW",
    "--redshift_schema": "spectrum_assistencia_ultron",
    "--schema": "estrutura_assistencia_ultron.json",
    "--redshift_key_path": "s3://dev-brasilseg-segbr-extracao/chaves-redis/ultron/assistencia",
    "--queries": "AssistenciaUltronQuerys.json",
    "--spark-event-logs-path": "s3://dev-brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_incremental/",
    "--type": "increment"
  },
  "Connections": {
    "Connections": [
      "Redshift_DataLake"
    ]
  },
  "WorkerType": "Standard",
  "NumberOfWorkers": 5,
  "GlueVersion": "2.0"
}' \
  --region us-east-1 \
  --profile dev
