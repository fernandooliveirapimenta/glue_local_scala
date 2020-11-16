
aws glue delete-job \
--job-name bb30_carga_assistencia_redis_ultron_incremental --region us-east-1


#-- hml scala

#bb30_carga_chaves_fortes_ultron
    #full
    aws glue create-job \
        --name bb30_carga_chaves_fortes_ultron \
        --role arn:aws:iam::057872281239:role/Brasilseg-GlueRole \
        --command '{
          "Name": "glueetl",
          "ScriptLocation": "s3://datalake-glue-scripts-hml/carga_incremental/etl15_chave_forte.scala"
        }' \
        --region us-east-1 \
        --output json \
        --default-arguments '{
             "--job-language":"scala",
            "--class":"GlueApp",
            "--TempDir": "s3://aws-glue-temporary-057872281239-us-east-1/admin",
            "--environment": "aws",
            "--database": "ultron",
            "--redshift_credentials": "hml/redshiftDW",
            "--keys": "id_oferta_plano;id_proposta;id_evento_timeline",
            "--schemas":"s3://datalake-glue-scripts-hml/estrutura_tabela/bb30/estrutura_assistencia_ultron.json;s3://datalake-glue-scripts-hml/estrutura_tabela/bb30/estrutura_proposta_ultron.json;s3://datalake-glue-scripts-hml/estrutura_tabela/bb30/estrutura_evento_sinistro_ultron.json",
            "--entities":"assistencia;proposta;evento_sinistro",
            "--target_key_path":"s3://hml-brasilseg-segbr-extracao/chaves-redis/%s/%s"
            }' \
        --connections '{"Connections": ["redshift_dw"]}'\
        --tags '{
                   "SubProjeto":"bb30",
                   "UnidadeDeNegocios":"massificados",
                   "Ambiente":"homologacao",
                   "Projeto":"bb30",
                   "TipoAmbiente":"etl"
               }' \
        --profile hml \
        --endpoint https://glue.us-east-1.amazonaws.com\
        --glue-version '2.0'\
        --number-of-workers '5'\
        --worker-type 'Standard'


