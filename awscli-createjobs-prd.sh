
aws glue delete-job \
--job-name bb30_carga_assistencia_redis_ultron_incremental --region us-east-1

# PRD scala
    #full
    aws glue create-job \
        --name bb30_carga_assistencia_redis_ultron_historico \
        --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role \
        --command '{
          "Name": "glueetl",
          "ScriptLocation": "s3://datalake-glue-scripts/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala"
        }' \
        --region us-east-1 \
        --output json \
        --default-arguments '{
             "--job-language":"scala",
            "--extra-jars": "s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts/dependencias_scala/Assistencia_ABS/entityDependencies.jar",
            "--extra-files": "s3://datalake-glue-scripts/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts/estrutura_query/AssistenciaUltronQuerys.json",
            "--class":"GlueApp",
            "--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin",
            "--conf": "spark.driver.memory=10g",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-s3-parquet-optimized-committer": "true",
            "--enable-spark-ui": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--enable-metrics":"true",
            "--environment": "aws",
            "--redis_db": "2",
            "--database": "ultron",
            "--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint",
            "--redis_port": "/brseg/elasticache/brasilseg-redis/porta",
            "--redis_auth": "/brseg/elasticache/brasilseg-redis/token",
            "--redis_ssl": "true",
            "--filter": "%CARGA_BB_3.0%",
            "--redshift_credentials": "prd/redshiftDW",
            "--redshift_schema": "spectrum_assistencia_ultron",
            "--schema":"estrutura_assistencia_ultron.json",
            "--redshift_key_path": "s3://brasilseg-segbr-extracao/chaves-redis/ultron/assistencia",
            "--queries":"AssistenciaUltronQuerys.json",
            "--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_historico/",
            "--type": "full"}' \
        --connections '{"Connections": ["dw_db"]}'\
        --tags '{
                   "SubProjeto":"bb30",
                   "UnidadeDeNegocios":"massificados",
                   "Ambiente":"producao",
                   "Projeto":"bb30",
                   "TipoAmbiente":"etl"
               }' \
        --profile prd \
        --endpoint https://glue.us-east-1.amazonaws.com\
        --glue-version '2.0'\
        --number-of-workers '5'\
        --worker-type 'Standard'


    #increment
    aws glue create-job \
        --name bb30_carga_assistencia_redis_ultron_incremental \
        --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role \
        --command '{
          "Name": "glueetl",
          "ScriptLocation": "s3://datalake-glue-scripts/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala"
        }' \
        --region us-east-1 \
        --output json \
        --default-arguments '{
             "--job-language":"scala",
            "--extra-jars": "s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts/dependencias_scala/Assistencia_ABS/entityDependencies.jar",
            "--extra-files": "s3://datalake-glue-scripts/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts/estrutura_query/AssistenciaUltronQuerys.json",
            "--class":"GlueApp",
            "--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin",
            "--conf": "spark.driver.memory=10g",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-s3-parquet-optimized-committer": "true",
            "--enable-spark-ui": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--enable-metrics":"true",
            "--environment": "aws",
            "--redis_db": "2",
            "--database": "ultron",
            "--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint",
            "--redis_port": "/brseg/elasticache/brasilseg-redis/porta",
            "--redis_auth": "/brseg/elasticache/brasilseg-redis/token",
            "--redis_ssl": "true",
            "--filter": "%CARGA_BB_3.0%",
            "--redshift_credentials": "prd/redshiftDW",
            "--redshift_schema": "spectrum_assistencia_ultron",
            "--schema":"estrutura_assistencia_ultron.json",
            "--redshift_key_path": "s3://brasilseg-segbr-extracao/chaves-redis/ultron/assistencia",
            "--queries":"AssistenciaUltronQuerys.json",
            "--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_incremental/",
            "--type": "increment"}' \
        --connections '{"Connections": ["dw_db"]}'\
        --tags '{
                   "SubProjeto":"bb30",
                   "UnidadeDeNegocios":"massificados",
                   "Ambiente":"producao",
                   "Projeto":"bb30",
                   "TipoAmbiente":"etl"
               }' \
        --profile prd \
        --endpoint https://glue.us-east-1.amazonaws.com\
        --glue-version '2.0'\
        --number-of-workers '5'\
        --worker-type 'Standard'

