#!/bin/bash

#aws s3 cp target/*.jar s3://hml-emr-script/scripts/ --profile controlm

echo 'movendo script'
aws s3 cp src/main/scala/etlfinal/assistencia_db_carga_redis_unificada.scala s3://datalake-glue-scripts-dev/carga_historica/carga_redis/ --profile dev
echo 'movendo estrutura_tabela'
aws s3 cp estrutura_assistencia_ultron.json s3://datalake-glue-scripts-dev/estrutura_tabela/ --profile dev
