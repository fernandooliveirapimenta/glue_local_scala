profile=$1

echo "Proposta BB3.0 ..."

cd script

#Todo validar se isso pode subir
#./glue.sh job-update-parameters --job-name bb30_carga_proposta_redis_preparacao --parameters "{'Command':{'ScriptLocation':'s3://datalake-glue-scripts/carga_historica/carga_redis/carga_propostas_preparacao.scala'},'DefaultArguments':{'--extra-jars':'s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar','--system':'segbr;segbr;ultron','--database':'ab_mobile;abs_mobile;ultron_apolice','--categories':'vida;residencial;rural_ab;rural_abs;prestamista;ultron_residencial','--redshift_key_path':'#deprecated'}}" --region us-east-1 --profile $profile
#./glue.sh job-update-parameters --job-name bb30_carga_proposta_redis_home --parameters "{'DefaultArguments':{'--category':'vida,residencial,rural_ab,rural_abs,prestamista,ultron_residencial'}}" --region us-east-1 --profile $profile

aws glue create-job --name bb30_carga_proposta_redis_ultron_residencial_historico --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role --execution-property MaxConcurrentRuns=1 --command Name=glueetl,ScriptLocation=s3://datalake-glue-scripts/carga_historica/carga_redis/carga_propostas_unificada_cat.scala,PythonVersion=2 --connections Connections=dw_db --max-retries 0 --allocated-capacity 11 --glue-version 1.0 --default-arguments '{"--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin","--category": "ultron_residencial","--class": "GlueApp","--conf": "spark.driver.maxResultSize=10g","--enable-continuous-cloudwatch-log": "true","--enable-metrics": "","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-files": "s3://datalake-glue-scripts/estrutura_query/queries_propostas_unificada_cat.json","--extra-jars": "s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar","--home": "false","--job-bookmark-option": "job-bookmark-disable","--job-language": "scala","--queries": "queries_propostas_unificada_cat.json","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "7","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "prd/redshiftDW","--redshift_key_path": "null","--database": "ultron","--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_proposta_redis_ultron_residencial_historico/","--type": "full"}' --tags '{"SubProjeto":"bb30","UnidadeDeNegocios":"massificados","Ambiente":"producao","Projeto":"bb30","TipoAmbiente":"etl"}' --region us-east-1 --profile $profile
aws glue create-job --name bb30_carga_proposta_redis_ultron_residencial_incremental --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role --execution-property MaxConcurrentRuns=1 --command Name=glueetl,ScriptLocation=s3://datalake-glue-scripts/carga_historica/carga_redis/carga_propostas_unificada_cat.scala,PythonVersion=2 --connections Connections=dw_db --max-retries 0 --allocated-capacity 11 --glue-version 1.0 --default-arguments '{"--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin","--category": "ultron_residencial","--class": "GlueApp","--enable-continuous-cloudwatch-log": "true","--enable-metrics": "","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-files": "s3://datalake-glue-scripts/estrutura_query/queries_propostas_unificada_cat.json","--extra-jars": "s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar","--home": "false","--job-bookmark-option": "job-bookmark-disable","--job-language": "scala","--queries": "queries_propostas_unificada_cat.json","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "7","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "prd/redshiftDW","--redshift_key_path": "s3://brasilseg-segbr-extracao/chaves-redis/ultron/proposta","--database": "ultron","--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_proposta_redis_ultron_residencial_incremental/","--type": "increment"}' --tags '{"SubProjeto":"bb30","UnidadeDeNegocios":"massificados","Ambiente":"producao","Projeto":"bb30","TipoAmbiente":"etl"}' --region us-east-1 --profile $profile

echo "Assistência BB3.0 ..."

aws glue create-job --name bb30_carga_assistencia_redis_ultron_historico --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role --execution-property MaxConcurrentRuns=1 --command Name=glueetl,ScriptLocation=s3://datalake-glue-scripts/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala,PythonVersion=3 --connections Connections=dw_db --max-retries 0 --allocated-capacity 11 --glue-version 2.0 --default-arguments '{"--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin","--class": "GlueApp","--conf": "spark.driver.memory=10g","--enable-continuous-cloudwatch-log": "true","--enable-metrics": "","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-files": "s3://datalake-glue-scripts/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts/estrutura_query/AssistenciaUltronQuerys.json","--extra-jars": "s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts/dependencias_scala/Assistencia_ABS/entityDependencies.jar","--filter": "%CARGA_BB_3.0%","--job-bookmark-option": "job-bookmark-disable","--job-language": "scala","--queries": "AssistenciaUltronQuerys.json","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "2","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "prd/redshiftDW","--redshift_schema": "spectrum_assistencia_ultron","--schema": "estrutura_assistencia_ultron.json","--redshift_key_path": "null","--database": "ultron","--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_historico/","--type": "full"}' --tags '{"SubProjeto":"bb30","UnidadeDeNegocios":"massificados","Ambiente":"producao","Projeto":"bb30","TipoAmbiente":"etl"}' --region us-east-1 --profile $profile
aws glue create-job --name bb30_carga_assistencia_redis_ultron_incremental --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role --execution-property MaxConcurrentRuns=1 --command Name=glueetl,ScriptLocation=s3://datalake-glue-scripts/carga_historica/carga_redis/assistencia_db_carga_redis_unificada.scala,PythonVersion=3 --connections Connections=dw_db --max-retries 0 --allocated-capacity 11 --glue-version 2.0 --default-arguments '{"--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin","--class": "GlueApp","--conf": "spark.driver.memory=10g","--enable-continuous-cloudwatch-log": "true","--enable-metrics": "","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-files": "s3://datalake-glue-scripts/estrutura_tabela/estrutura_assistencia_ultron.json,s3://datalake-glue-scripts/estrutura_query/AssistenciaUltronQuerys.json","--extra-jars": "s3://datalake-glue-scripts/dependencias_scala/jedis-combined-1.0.jar,s3://datalake-glue-scripts/dependencias_scala/Assistencia_ABS/entityDependencies.jar","--filter": "%CARGA_BB_3.0%","--job-bookmark-option": "job-bookmark-disable","--job-language": "scala","--queries": "AssistenciaUltronQuerys.json","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "2","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "prd/redshiftDW","--redshift_schema": "spectrum_assistencia_ultron","--schema": "estrutura_assistencia_ultron.json","--redshift_key_path": "s3://brasilseg-segbr-extracao/chaves-redis/ultron/assistencia","--database": "ultron","--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_assistencia_redis_ultron_incremental/","--type": "increment"}' --tags '{"SubProjeto":"bb30","UnidadeDeNegocios":"massificados","Ambiente":"producao","Projeto":"bb30","TipoAmbiente":"etl"}' --region us-east-1 --profile $profile

echo "Evento Sinistro BB3.0 ..."

aws glue create-job --name bb30_carga_evento_sinistro_redis_ultron_historico --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role --execution-property MaxConcurrentRuns=1 --command Name=glueetl,ScriptLocation=s3://datalake-glue-scripts/carga_historica/carga_redis/carga_evento_sinistro_redis_ultron.py,PythonVersion=3 --connections Connections=dw_db --max-retries 0 --allocated-capacity 11 --glue-version 1.0 --default-arguments '{"--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin","--conf": "spark.driver.memory=10g","--enable-continuous-cloudwatch-log": "true","--enable-metrics": "","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-py-files": "s3://datalake-glue-scripts/dependencias_python/redis.zip","--job-bookmark-option": "job-bookmark-disable","--job-language": "python","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "3","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "prd/redshiftDW","--redshift_key_path": "null","--database": "ultron","--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_evento_sinistro_redis_ultron_historico/","--type": "full"}' --tags '{"SubProjeto":"bb30","UnidadeDeNegocios":"massificados","Ambiente":"producao","Projeto":"bb30","TipoAmbiente":"etl"}' --region us-east-1 --profile $profile
aws glue create-job --name bb30_carga_evento_sinistro_redis_ultron_incremental --role arn:aws:iam::909530209831:role/Brasilseg-Glue-Role --execution-property MaxConcurrentRuns=1 --command Name=glueetl,ScriptLocation=s3://datalake-glue-scripts/carga_historica/carga_redis/carga_evento_sinistro_redis_ultron.py,PythonVersion=3 --connections Connections=dw_db --max-retries 0 --allocated-capacity 11 --glue-version 1.0 --default-arguments '{"--TempDir": "s3://aws-glue-temporary-909530209831-us-east-1/admin","--conf": "spark.driver.memory=10g","--enable-continuous-cloudwatch-log": "true","--enable-metrics": "","--enable-s3-parquet-optimized-committer": "true","--enable-spark-ui": "true","--environment": "aws","--extra-py-files": "s3://datalake-glue-scripts/dependencias_python/redis.zip","--job-bookmark-option": "job-bookmark-disable","--job-language": "python","--redis_auth": "/brseg/elasticache/brasilseg-redis/token","--redis_db": "3","--redis_host": "/brseg/elasticache/brasilseg-redis/endpoint","--redis_port": "/brseg/elasticache/brasilseg-redis/porta","--redis_ssl": "true","--redshift_credentials": "prd/redshiftDW","--redshift_key_path": "s3://brasilseg-segbr-extracao/chaves-redis/ultron/evento_sinistro","--database": "ultron","--spark-event-logs-path": "s3://brasilseg-glue-log/bb30_carga_evento_sinistro_redis_ultron_incremental/","--type": "increment"}' --tags '{"SubProjeto":"bb30","UnidadeDeNegocios":"massificados","Ambiente":"producao","Projeto":"bb30","TipoAmbiente":"etl"}' --region us-east-1 --profile $profile
