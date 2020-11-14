echo ("Conectando no batabase do Redshift ...");

connect ("jdbc:redshift://cluster-dw.catcuc4korzp.us-east-1.redshift.amazonaws.com:5439/dw_db", "admin", "Brasilseg#&!6453!");

echo ("Criando os schemas spectrum 909530209831 ...");

create external schema spectrum_assistencia_ultron from data catalog database 'data_lake_ultron_assistencia' region 'us-east-1' iam_role 'arn:aws:iam::909530209831:role/Brasilseg-RedshiftS3Role'; 
create external schema spectrum_apolice_ultron from data catalog database 'data_lake_ultron_apolice' region 'us-east-1' iam_role 'arn:aws:iam::909530209831:role/Brasilseg-RedshiftS3Role'; 
create external schema spectrum_sinistro_ultron from data catalog database 'data_lake_ultron_sinistro' region 'us-east-1' iam_role 'arn:aws:iam::909530209831:role/Brasilseg-RedshiftS3Role'; 
create external schema spectrum_cliente_ultron from data catalog database 'data_lake_ultron_cliente' region 'us-east-1' iam_role 'arn:aws:iam::909530209831:role/Brasilseg-RedshiftS3Role'; 
create external schema spectrum_produto_ultron from data catalog database 'data_lake_ultron_produto' region 'us-east-1' iam_role 'arn:aws:iam::909530209831:role/Brasilseg-RedshiftS3Role';

echo ("Criando as grants usage ...");

grant usage on schema spectrum_assistencia_ultron to group data_readers;
grant usage on schema spectrum_apolice_ultron to group data_readers;
grant usage on schema spectrum_sinistro_ultron to group data_readers;
grant usage on schema spectrum_cliente_ultron to group data_readers;
grant usage on schema spectrum_produto_ultron to group data_readers; 

echo ("Criando as grants select ...");

grant select on all tables in schema spectrum_assistencia_ultron to group data_readers;
grant select on all tables in schema spectrum_apolice_ultron to group data_readers;
grant select on all tables in schema spectrum_sinistro_ultron to group data_readers;
grant select on all tables in schema spectrum_cliente_ultron to group data_readers;
grant select on all tables in schema spectrum_produto_ultron to group data_readers; 


disconnect ();
