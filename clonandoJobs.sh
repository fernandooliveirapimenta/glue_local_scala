
#assistencia prd
./glue.sh job-clone --job-name-source carga_assistencia_redis_ab_mobile_assistencia_db\
 --region-source us-east-1 --profile-source prd --job-name-target carga_assistencia_redis_ab_mobile_assistencia_db\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_assistencia_redis_ab_mobile_assistencia_db_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target carga_assistencia_redis_ab_mobile_assistencia_db_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_assistencia_redis_abs_mobile_assistencia_db\
 --region-source us-east-1 --profile-source prd --job-name-target carga_assistencia_redis_abs_mobile_assistencia_db\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_assistencia_redis_abs_mobile_assistencia_db_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target carga_assistencia_redis_abs_mobile_assistencia_db_incremental\
  --region-target us-east-1 --profile-target pessoal


#proposta prd
./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_home\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_home\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_prestamista_historico\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_prestamista_historico\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_prestamista_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_prestamista_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_residencial_historico\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_residencial_historico\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_residencial_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_residencial_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_rural_ab_historico\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_rural_ab_historico\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_rural_ab_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_rural_ab_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_rural_abs_historico\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_rural_abs_historico\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_rural_abs_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_rural_abs_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_vida_historico\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_vida_historico\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_vida_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_vida_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source bb30_carga_proposta_redis_preparacao\
 --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_preparacao\
  --region-target us-east-1 --profile-target pessoal

aws iam create-policy --policy-name SSMGetParameterAccess --policy-document '{    "Version": "2012-10-17",    "Statement": [        {            "Sid": "VisualEditor0",            "Effect": "Allow",            "Action": [                "ssm:DescribeParameters",                "ssm:GetParameters",                "ssm:GetParameter"            ],            "Resource": "*"        }    ]}' --profile pessoal
aws iam attach-role-policy --policy-arn arn:aws:iam::646025261134:policy/SSMGetParameterAccess --role-name Brasilseg-Glue-Role --profile pessoal

# evento sinistro
./glue.sh job-clone --job-name-source carga_evento_sinistro_redis_ab_mobile_seguros_db\
 --region-source us-east-1 --profile-source prd --job-name-target carga_evento_sinistro_redis_ab_mobile_seguros_db\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_evento_sinistro_redis_ab_mobile_seguros_db_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target carga_evento_sinistro_redis_ab_mobile_seguros_db_incremental\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_evento_sinistro_redis_abs_mobile_seguros_db\
 --region-source us-east-1 --profile-source prd --job-name-target carga_evento_sinistro_redis_abs_mobile_seguros_db\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_evento_sinistro_redis_abs_mobile_seguros_db_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target carga_evento_sinistro_redis_abs_mobile_seguros_db_incremental\
  --region-target us-east-1 --profile-target pessoal


echo "Sinistro BB3.0"

./glue.sh job-clone --job-name-source carga_sinistro_redis_abs_mobile_seguros_db\
 --region-source us-east-1 --profile-source prd --job-name-target carga_sinistro_redis_abs_mobile_seguros_db\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source carga_sinistro_redis_abs_mobile_seguros_db_incremental\
 --region-source us-east-1 --profile-source prd --job-name-target carga_sinistro_redis_abs_mobile_seguros_db_incremental\
  --region-target us-east-1 --profile-target pessoal


echo "Aviso Web"

./glue.sh job-clone --job-name-source asw_carga_redis_ab_seguros_db_incremental_async_proposta\
 --region-source us-east-1 --profile-source prd --job-name-target asw_carga_redis_ab_seguros_db_incremental_async_proposta\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source asw_carga_redis_ab_seguros_db_incremental_async_sinistro\
 --region-source us-east-1 --profile-source prd --job-name-target asw_carga_redis_ab_seguros_db_incremental_async_sinistro\
  --region-target us-east-1 --profile-target pessoal

./glue.sh job-clone --job-name-source asw_carga_redis_ab_seguros_db_historico_etl\
 --region-source us-east-1 --profile-source prd --job-name-target asw_carga_redis_ab_seguros_db_historico_etl\
  --region-target us-east-1 --profile-target pessoal

asw_carga_redis_ab_seguros_db_historico_etl
asw_carga_redis_ab_seguros_db_incremental_async_proposta
asw_carga_redis_ab_seguros_db_incremental_async_sinistro
bb30_carga_assistencia_redis_ultron_historico
bb30_carga_assistencia_redis_ultron_incremental
bb30_carga_evento_sinistro_redis_ultron_historico
bb30_carga_evento_sinistro_redis_ultron_incremental
bb30_carga_proposta_redis_home
bb30_carga_proposta_redis_preparacao
bb30_carga_proposta_redis_prestamista_historico
bb30_carga_proposta_redis_prestamista_incremental
bb30_carga_proposta_redis_residencial_historico
bb30_carga_proposta_redis_residencial_incremental
bb30_carga_proposta_redis_rural_ab_historico
bb30_carga_proposta_redis_rural_ab_incremental
bb30_carga_proposta_redis_rural_abs_historico
bb30_carga_proposta_redis_rural_abs_incremental
bb30_carga_proposta_redis_ultron_residencial_historico
bb30_carga_proposta_redis_ultron_residencial_incremental
bb30_carga_proposta_redis_vida_historico
bb30_carga_proposta_redis_vida_incremental
bb30_carga_sinistro_redis_ab_historico
bb30_carga_sinistro_redis_ab_incremental
carga_assistencia_redis_ab_mobile_assistencia_db
carga_assistencia_redis_ab_mobile_assistencia_db_incremental
carga_assistencia_redis_abs_mobile_assistencia_db
carga_assistencia_redis_abs_mobile_assistencia_db_incremental
carga_evento_sinistro_redis_ab_mobile_seguros_db
carga_evento_sinistro_redis_ab_mobile_seguros_db_incremental
carga_evento_sinistro_redis_abs_mobile_seguros_db
carga_evento_sinistro_redis_abs_mobile_seguros_db_incremental
carga_sinistro_redis_abs_mobile_seguros_db
carga_sinistro_redis_abs_mobile_seguros_db_incremental















