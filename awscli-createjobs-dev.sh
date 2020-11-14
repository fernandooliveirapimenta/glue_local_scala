
#-- DEV scala
aws glue delete-job \
--job-name bb30_carga_assistencia_redis_ultron_incremental --region us-east-1

#proposta
aws glue get-job --job-name bb30_carga_proposta_redis_home --region us-east-1 --profile dev --output json > dev.txt && aws glue get-job --job-name bb30_carga_proposta_redis_home --region us-east-1 --profile cloudwatch --output json > prd.txt && diff -a --suppress-common-lines -y dev.txt prd.txt
aws glue get-job --job-name bb30_carga_proposta_redis_home --region us-east-1 --profile dev --output json > dev.txt && aws glue get-job --job-name bb30_carga_proposta_redis_home --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt
aws glue get-job --job-name bb30_carga_proposta_redis_prestamista_historico --region us-east-1 --profile dev --output json > dev.txt && aws glue get-job --job-name bb30_carga_proposta_redis_prestamista_historico --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt
aws glue get-job --job-name bb30_carga_proposta_redis_prestamista_incremental --region us-east-1 --profile dev --output json > dev.txt && aws glue get-job --job-name bb30_carga_proposta_redis_prestamista_incremental --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_home \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_home \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_preparacao \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_preparacao \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_residencial_historico \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_residencial_historico \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

*"--conf": "spark.driver.maxResultSize=10g"
aws glue get-job --job-name bb30_carga_proposta_redis_residencial_incremental \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_residencial_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

,'--enable-s3-parquet-optimized-committer': 'true'
aws glue get-job --job-name bb30_carga_proposta_redis_rural_ab_historico \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_rural_ab_historico \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_rural_ab_incremental \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_rural_ab_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_rural_abs_historico \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_rural_abs_historico \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_rural_abs_incremental \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_rural_abs_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_vida_historico \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_vida_historico \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name bb30_carga_proposta_redis_vida_incremental \
--region us-east-1 --profile dev --output json > dev.txt && \
 aws glue get-job --job-name bb30_carga_proposta_redis_vida_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

#evento

aws glue get-job --job-name carga_evento_sinistro_redis_ab_mobile_seguros_db \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_evento_sinistro_redis_ab_mobile_seguros_db \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name carga_evento_sinistro_redis_ab_mobile_seguros_db_incremental \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_evento_sinistro_redis_ab_mobile_seguros_db_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name carga_evento_sinistro_redis_abs_mobile_seguros_db_incremental \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_evento_sinistro_redis_abs_mobile_seguros_db_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt


# Sinistro
aws glue get-job --job-name carga_sinistro_redis_ab_mobile_seguros_db_historico \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_sinistro_redis_ab_seguros_db_historico \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name carga_sinistro_redis_ab_mobile_seguros_db_incremental \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_sinistro_redis_ab_seguros_db_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db_incremental \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db_incremental \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

# Aviso Web
aws glue get-job --job-name asw_carga_redis_ab_seguros_db_incremental_propostas \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name asw_carga_redis_ab_seguros_db_incremental_async_proposta \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name asw_carga_redis_ab_seguros_db_incremental_sinistros \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name asw_carga_redis_ab_seguros_db_incremental_async_sinistro \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

aws glue get-job --job-name asw_carga_redis_ab_seguros_db_historico \
--region us-east-1 --profile hml --output json > dev.txt && \
 aws glue get-job --job-name asw_carga_redis_ab_seguros_db_historico_etl \
  --region us-east-1 --profile cloudwatch --output json > prd.txt && vimdiff dev.txt prd.txt

#bb30_carga_assistencia_redis_ultron_historico
    #full
    aws glue create-job \
        --name bb30_carga_assistencia_redis_ultron_historico \
        --role arn:aws:iam::533865883733:role/Brasilseg-GlueRole \
        --command '{
          "Name": "glueetl",
          "ScriptLocation": "s3://datalake-glue-scripts-dev/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala"
        }' \
        --region us-east-1 \
        --output json \
        --default-arguments '{
             "--job-language":"scala",
            "--extra-jars": "s3://datalake-glue-scripts-dev/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts-dev/dependencias_scala/Assistencia_ABS/entityDependencies.jar",
            "--extra-files": "s3://datalake-glue-scripts-dev/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts-dev/estrutura_query/AssistenciaUltronQuerys.json",
            "--class":"GlueApp",
            "--TempDir": "s3://aws-glue-temporary-533865883733-us-east-1/admin",
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
            "--redshift_credentials": "dev/redshiftDW",
            "--redshift_schema": "spectrum_assistencia_ultron",
            "--schema":"estrutura_assistencia_ultron.json",
            "--redshift_key_path": "s3://dev-brasilseg-segbr-extracao/chaves-redis/ultron/assistencia",
            "--queries":"AssistenciaUltronQuerys.json",
            "--spark-event-logs-path": "s3://dev-brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_historico/",
            "--type": "full"}' \
        --connections '{"Connections": ["Redshift_DataLake"]}'\
        --tags '{
                   "SubProjeto":"bb30",
                   "UnidadeDeNegocios":"massificados",
                   "Ambiente":"desenvolvimento",
                   "Projeto":"bb30",
                   "TipoAmbiente":"etl"
               }' \
        --profile dev \
        --endpoint https://glue.us-east-1.amazonaws.com\
        --glue-version '2.0'\
        --number-of-workers '5'\
        --worker-type 'Standard'


    #increment
    aws glue create-job \
        --name bb30_carga_assistencia_redis_ultron_incremental \
        --role arn:aws:iam::533865883733:role/Brasilseg-GlueRole \
        --command '{
          "Name": "glueetl",
          "ScriptLocation": "s3://datalake-glue-scripts-dev/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala"
        }' \
        --region us-east-1 \
        --output json \
        --default-arguments '{
             "--job-language":"scala",
            "--extra-jars": "s3://datalake-glue-scripts-dev/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts-dev/dependencias_scala/Assistencia_ABS/entityDependencies.jar",
            "--extra-files": "s3://datalake-glue-scripts-dev/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts-dev/estrutura_query/AssistenciaUltronQuerys.json",
            "--class":"GlueApp",
            "--TempDir": "s3://aws-glue-temporary-533865883733-us-east-1/admin",
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
            "--redshift_credentials": "dev/redshiftDW",
            "--redshift_schema": "spectrum_assistencia_ultron",
            "--schema":"estrutura_assistencia_ultron.json",
            "--redshift_key_path": "s3://dev-brasilseg-segbr-extracao/chaves-redis/ultron/assistencia",
            "--queries":"AssistenciaUltronQuerys.json",
            "--spark-event-logs-path": "s3://dev-brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_incremental/",
            "--type": "increment"}' \
        --connections '{"Connections": ["Redshift_DataLake"]}'\
        --tags '{
                   "SubProjeto":"bb30",
                   "UnidadeDeNegocios":"massificados",
                   "Ambiente":"desenvolvimento",
                   "Projeto":"bb30",
                   "TipoAmbiente":"etl"
               }' \
        --profile dev \
        --endpoint https://glue.us-east-1.amazonaws.com\
        --glue-version '2.0'\
        --number-of-workers '5'\
        --worker-type 'Standard'


# DEV Python

        aws glue create-job \
    --name bb30_carga_evento_sinistro_redis_ultron_historico \
    --role arn:aws:iam::533865883733:role/Brasilseg-GlueRole \
    --command '{"Name": "glueetl","ScriptLocation": "s3://datalake-glue-scripts-dev/carga_historica/carga_redis/carga_evento_sinistro_redis_ultron.py","PythonVersion": "3"}' \
    --region us-east-1 \
    --output json \
    --default-arguments '{"--TempDir": "s3://aws-glue-temporary-533865883733-us-east-1/admin","--conf": "spark.driver.memory=10g","--enable-continuous-cloudwatch-log": "true","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-py-files": "s3://datalake-glue-scripts-dev/dependencias_python/redis.zip","--job-bookmark-option": "job-bookmark-disable","--job-language": "python","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "3","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "dev/redshiftDW","--spark-event-logs-path": "s3://dev-brasilseg-glue-log/bb30_carga_evento_sinistro_redis_ultron_historico/","--type": "full"}' \
    --connections '{"Connections": ["Redshift_DataLake"]}'\
    --profile DEV \
    --endpoint https://glue.us-east-1.amazonaws.com


