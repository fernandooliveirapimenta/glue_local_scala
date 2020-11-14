aws glue get-job --job-name asw_carga_redis_ab_seguros_db_historico_etl \
--region us-east-1 --profile prd --output json > dev.txt && \
 aws glue get-job --job-name asw_carga_redis_ab_seguros_db_historico_etl \
  --region us-east-1 --profile pessoal --output json > prd.txt && vimdiff dev.txt prd.txt



aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db \
--region us-east-1 --profile prd --output json > dev.txt && \
 aws glue get-job --job-name carga_sinistro_redis_abs_mobile_seguros_db \
  --region us-east-1 --profile pessoal --output json > prd.txt && vimdiff dev.txt prd.txt

python3.7 glue.py job-update-parameters --job-name carga_assistencia_redis_ab_mobile_assistencia_db --parameters "{'Command':{'ScriptLocation':'s3://datalake-glue-scripts/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala'},'DefaultArguments':{'--extra-jars':'s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts/dependencias_scala/Assistencia_AB/entityDependencies.jar','--extra-files': 's3://datalake-glue-scripts/estrutura_tabela/estrutura_assistencia_ab_mobile.json,s3://datalake-glue-scripts/estrutura_query/AssistenciaABQuerys.json','--redis_host':'/brseg/elasticache/brasilseg-redis/endpoint','--redis_port':'/brseg/elasticache/brasilseg-redis/porta','--redis_ssl':'true','--redis_auth':'/brseg/elasticache/brasilseg-redis/token','--environment':'aws','--database': 'ab','--redshift_schema': 'spectrum_assistencia_db_ab_mobile','--schema': 'estrutura_assistencia_ab_mobile.json','--queries': 'AssistenciaABQuerys.json','--enable-spark-ui': 'true','--conf': 'spark.driver.maxResultSize=10g','--spark-event-logs-path': 's3://brasilseg-glue-log/carga_assistencia_redis_ab_mobile_assistencia_db/','--enable-s3-parquet-optimized-committer':'true'},'GlueVersion': '2.0'}" --region us-east-1 --profile $profile

./glue.sh job-update-parameters --job-name carga_assistencia_redis_ab_mobile_assistencia_db --parameters "{'Command':{'ScriptLocation':'s3://datalake-glue-scripts/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala'},'DefaultArguments':{'--extra-jars':'s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts/dependencias_scala/Assistencia_AB/entityDependencies.jar','--extra-files': 's3://datalake-glue-scripts/estrutura_tabela/estrutura_assistencia_ab_mobile.json,s3://datalake-glue-scripts/estrutura_query/AssistenciaABQuerys.json','--redis_host':'/brseg/elasticache/brasilseg-redis/endpoint','--redis_port':'/brseg/elasticache/brasilseg-redis/porta','--redis_ssl':'true','--redis_auth':'/brseg/elasticache/brasilseg-redis/token','--environment':'aws','--database': 'ab','--redshift_schema': 'spectrum_assistencia_db_ab_mobile','--schema': 'estrutura_assistencia_ab_mobile.json','--queries': 'AssistenciaABQuerys.json','--enable-spark-ui': 'true','--conf': 'spark.driver.maxResultSize=10g','--spark-event-logs-path': 's3://brasilseg-glue-log/carga_assistencia_redis_ab_mobile_assistencia_db/','--enable-s3-parquet-optimized-committer':'true'},'GlueVersion': '2.0'}" --region us-east-1 --profile pessoal

carga_assistencia_redis_ab_mobile_assistencia_db
carga_assistencia_redis_ab_mobile_assistencia_db_incremental
carga_assistencia_redis_abs_mobile_assistencia_db
carga_assistencia_redis_abs_mobile_assistencia_db_incremental
bb30_carga_proposta_redis_residencial_incremental
bb30_carga_proposta_redis_rural_ab_historico
bb30_carga_proposta_redis_vida_incremental
carga_evento_sinistro_redis_ab_mobile_seguros_db
carga_evento_sinistro_redis_ab_mobile_seguros_db_incremental
carga_evento_sinistro_redis_abs_mobile_seguros_db
carga_evento_sinistro_redis_abs_mobile_seguros_db_incremental
bb30_carga_sinistro_redis_ab_historico
bb30_carga_sinistro_redis_ab_incremental
carga_sinistro_redis_abs_mobile_seguros_db
carga_sinistro_redis_abs_mobile_seguros_db_incremental
asw_carga_redis_ab_seguros_db_incremental_async_proposta
asw_carga_redis_ab_seguros_db_incremental_async_sinistro
asw_carga_redis_ab_seguros_db_historico_etl

aws iam create-policy --policy-name SSMGetParameterAccess --policy-document '{"Version":"2012-10-17","Statement":[{"Sid":"VisualEditor0","Effect":"Allow","Action":["ssm:DescribeParameters","ssm:GetParameters","ssm:GetParameter"],"Resource":"*"}]}' --profile pessoal

aws iam attach-role-policy --policy-arn arn:aws:iam::909530209831:policy/SSMGetParameterAccess --role-name Brasilseg-Glue-Role --profile $profile


