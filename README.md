Projeto para desenvolver scripts hibridos para SPARK LOCAL / AWS GLUE

Depêndencias da IDLE:

Preferencia -> IntelliJ IDEA

- Instalar plugin *Scala* na IDLE

- Configurar *proxy* na IDLE...  *http://pac.bbmapfre.corp/config.pac*

- Utilizar *jdk 8* no SBT da IDLE


=====================================

New -> Project from Existing Sources -> select SBT & scala

=====================================

Configurações IntelliJ

=====================================

*Program Arguments ValidacaoQuantitativaDatalake*

--JOB_NAME="validacao_quantitativa_proposta_etl1"
--schema_path="s3://dev-brasilseg-segbr-extracao/estrutura_tabelas/estrutura_proposta.json"
--redshift_schema="spectrum_%s_abs"
--redshift_credentials="dev/redshiftDW"
--secret_manager_endpoint="secretsmanager.sa-east-1.amazonaws.com"
--secret_manager_region="sa-east-1"
--sqlserver_address="10.0.35.191"
--sqlserver_database="DESENV_ABS"
--sqlserver_user="SISBR"
--sqlserver_password="SISBR_QLD"
--csv_output_path="/tmp/validacao/proposta/"
--environment="local"

=====================================

*VM Options:*

-Dhttps.proxyHost="vbr008002-029.bbmapfre.corp"
-Dhttps.proxyPort="80"
-Dhttp.proxyHost="vbr008002-029.bbmapfre.corp"
-Dhttp.proxyPort="80"
-Dhttps.proxyUser=""
-Dhttps.proxyPassword=""
-Dhttp.proxyUser=""
-Dhttp.proxyPassword=""

