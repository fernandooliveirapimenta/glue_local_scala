import sys
import boto3
import redis
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from awsglue.job import Job

ssm = boto3.client('ssm', region_name='sa-east-1')

def get_aws_ssm_parameter(parameter):
    response = dict(ssm.get_parameter(
        Name=parameter,
        WithDecryption=True
    ))
    response_param = dict(response['Parameter'])
    return response_param['Value']

def get_aws_sm_value(secretKey, regionName):
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=regionName
    )
    try:
        responseCredentials = client.get_secret_value(
            SecretId=secretKey
        )
    except ClientError as e:
        raise e
    return json.loads(responseCredentials['SecretString'])

def write_redshift(df, dbTable):
    logger.info("Prepare table: {}".format(dbTable))

    df.write.format("com.databricks.spark.redshift").option("url",
                                                            "jdbc:redshift://{}:{}/{}".format(redshiftConfig["url"],
                                                                                              redshiftConfig["porta"],
                                                                                              redshiftConfig[
                                                                                                  "database"])).option(
        "tempdir", redshiftConfig["tempdir"]).option("aws_iam_role", redshiftConfig["aws_iam_role"]).option("dbtable",dbTable).option(
        "user", redshiftConfig["usuario"]).option("password", redshiftConfig["senha"]).mode('overwrite').save()

def query_to_data_frame(query):
    logger.info("Execute statement: {}".format(query))

    return sqlctx.read.format("com.databricks.spark.redshift").option("url", "jdbc:redshift://{}:{}/{}".format(
        redshiftConfig["url"], redshiftConfig["porta"], redshiftConfig["database"])).option(
        "tempdir", redshiftConfig["tempdir"]).option("aws_iam_role", redshiftConfig["aws_iam_role"]).option("query",
                                                                                                            query).option(
        "user", redshiftConfig["usuario"]).option("password", redshiftConfig["senha"]).load()

def weight(arr):
    months = {
        "JANEIRO": "01",
        "FEVEREIRO": "02",
        "MARÇO": "03",
        "ABRIL": "04",
        "MAIO": "05",
        "JUNHO": "06",
        "JULHO": "07",
        "AGOSTO": "08",
        "SETEMBRO": "09",
        "OUTUBRO": "10",
        "NOVEMBRO": "11",
        "DEZEMBRO": "12"
    }
    for item in arr:
        if not item["nome"]:
            item["w"] = 0
            continue
        spl = item["nome"].split("/")
        item["w"] = int(spl[1] + months.get(spl[0]))
    newarr = sorted(arr, key=lambda k: k["w"], reverse=True)
    i = 1
    for item in newarr:
        item["numeroOrdemExibicao"] = i
        item["detalhes"] = sorted(item["detalhes"], key=lambda k: (k["dia"], k["id"]), reverse=True)
        x = 1
        for detalhe in item["detalhes"]:
            detalhe["numeroOrdemExibicao"] = x
            x = x + 1
        i = i + 1
        del item["w"]
    return newarr

def enviar_sinistros(sinistros):
    salvar_no_redis_sinistros(sinistros)
    pipe.execute()

def enviar_sinistro_coberturas(sinistro_coberturas):
    salvar_no_redis_sinistro_coberturas(sinistro_coberturas)
    pipe.execute()

def enviar_sinistro_historicos(sinistro_historicos):
    salvar_no_redis_sinistro_historicos(sinistro_historicos)
    pipe.execute()

def enviar_sinistro_estimativa(sinistro_estimativa):
    salvar_no_redis_sinistro_estimativa(sinistro_estimativa)
    pipe.execute()

def enviar_sinistros_pagos(sinistros_pagos):
    salvar_no_redis_sinistros_pagos(sinistros_pagos)
    pipe.execute()

def enviar_documentos_solicitados(documentos_solicitados):
    salvar_no_redis_documentos_solicitados(documentos_solicitados)
    pipe.execute()

def enviar_documentos_complementar(documentos_complementar):
    salvar_no_redis_documentos_complementar(documentos_complementar)
    pipe.execute()

def enviar_vistoria_solicitada(vistoria_solicitada):
    definir_status_e_detalhes_vistoria_solicitada(vistoria_solicitada)
    pipe.execute()

def enviar_vistoria_concluida(vistoria_concluida):
    definir_status_e_detalhes_vistoria_concluida(vistoria_concluida)
    pipe.execute()

def enviar_vistoria_dispensada(vistoria_dispensada):
    definir_status_e_detalhes_vistoria_dispensada(vistoria_dispensada)
    pipe.execute()

def enviar_em_analise_reanalise(em_analise_reanalise):
    definir_status_e_detalhes_em_analise_reanalise(em_analise_reanalise)
    pipe.execute()

def enviar_em_analise(em_analise):
    definir_status_e_detalhes_em_analise(em_analise)
    pipe.execute()

def enviar_analise_finalizada(analise_finalizada):
    definir_status_e_detalhes_analise_finalizada(analise_finalizada)
    pipe.execute()

def enviar_processo_indeferido(processo_indeferido):
    definir_status_e_detalhes_processo_indeferido(processo_indeferido)
    pipe.execute()

def enviar_pagamento_efetuado(pagamento_efetuado):
    definir_status_e_detalhes_pagamento_efetuado(pagamento_efetuado)
    pipe.execute()

def enviar_aviso_finalizado(aviso_finalizado):
    definir_status_e_detalhes_aviso_finalizado(aviso_finalizado)
    pipe.execute()

def salvar_no_redis_sinistros(objetos):
    for objeto in objetos:

        logger.info("Gravando o sinistro {}".format(str(objeto["sinistroid"])))

        # Gravando o solicitante

        pipe.sadd('Solicitante', str(objeto["solicitanteid"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'solicitanteId', str(objeto["solicitanteid"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'nome', str(objeto["solicitantenome"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'endereco', str(objeto["endereco"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'bairro', str(objeto["bairro"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'municipio', str(objeto["municipio"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'estado', str(objeto["estado"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'cep', str(objeto["cep"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'contato.dddTelefone', str(objeto["dddtelefone"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'contato.telefone', str(objeto["telefone"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'contato.tipoTelefone', str(objeto["tipotelefone"]))
        pipe.hset('Solicitante:' + str(objeto["solicitanteid"]), 'contato.email', str(objeto["email"]))

        # Gravando Sinistrado

        if str(objeto["nome"]).strip() != "":
            pipe.sadd('Sinistrado', str(objeto["sinistroid"]))
            pipe.hset('Sinistrado:' + str(objeto["sinistroid"]), 'nome', str(objeto["nome"]))
            pipe.hset('Sinistrado:' + str(objeto["sinistroid"]), 'cpf', str(objeto["cpf"]))
            pipe.hset('Sinistrado:' + str(objeto["sinistroid"]), 'sexo', str(objeto["sexo"]))
            pipe.hset('Sinistrado:' + str(objeto["sinistroid"]), 'dataNascimento', str(objeto["dtnascimento"]))

        # Gravando o sinistro

        pipe.sadd('Sinistro', str(objeto["sinistroid"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'sinistroId', str(objeto["sinistroid"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'numeroProtocolo', str(objeto["numeroprotocolo"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'propostaId', str(objeto["propostaid"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'codObjetoSegurado', str(objeto["codobjetosegurado"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'eventoSinistro',
                  'EventoSinistro:' + str(objeto["eventosinistroid"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'dataAbertura', str(objeto["dataabertura"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'dataOcorrencia', str(objeto["dataocorrencia"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'situacao', str(objeto["situacao"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'sinistroBancoBrasil', str(objeto["sinistrobancobrasil"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'valorEstimado', str(objeto["valorestimado"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'valorPago', str(objeto["valorpago"]))
        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'solicitante', 'Solicitante:' + str(objeto["solicitanteid"]))

        if str(objeto["nome"]).strip() != "":
            pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'sinistrado', 'Sinistrado:' + str(objeto["sinistroid"]))

        # Criando índice para o sinistro gravado

        pipe.sadd('Sinistro:' + str(objeto["sinistroid"]) + ':idx', 'Sinistro:propostaId:' + str(objeto["propostaid"]))
        pipe.sadd('Sinistro:propostaId:' + str(objeto["propostaid"]), str(objeto["sinistroid"]))

        # Tratando o comunicado associado ao sinistro gravado

        codigoAgrupadorRef = ""
        etapaComunicado = ""

        if str(objeto["numeroprotocolo"]).strip() != "":

            # Criando índice para o sinistro gravado

            pipe.sadd('Sinistro:' + str(objeto["sinistroid"]) + ':idx',
                      'Sinistro:numeroProtocolo:' + str(objeto["numeroprotocolo"]))
            pipe.sadd('Sinistro:numeroProtocolo:' + str(objeto["numeroprotocolo"]), str(objeto["sinistroid"]))

            comunicadoExiste = str(redis.exists('ComunicadoSinistro:' + str(objeto["numeroprotocolo"])))
            if comunicadoExiste == "1":

                # Recuperando o código agrupador do comunicado associado ao sinistro gravado

                campoExiste = str(
                    redis.hexists('ComunicadoSinistro:' + str(objeto["numeroprotocolo"]), 'codigoAgrupador'))
                # hexists retorna bolean
                if campoExiste == "True":
                    codigoAgrupadorRef = redis.hget('ComunicadoSinistro:' + str(objeto["numeroprotocolo"]),
                                                    'codigoAgrupador')

                    # Decode pois esta vindo como bytes
                    codigoAgrupadorRef = codigoAgrupadorRef.decode("utf-8")
                    pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'codigoAgrupador', str(codigoAgrupadorRef))

                    # Criando índice para o sinistro gravado
                    pipe.sadd('Sinistro:' + str(objeto["sinistroid"]) + ':idx',
                              'Sinistro:codigoAgrupador:' + str(codigoAgrupadorRef))
                    pipe.sadd('Sinistro:codigoAgrupador:' + str(codigoAgrupadorRef), str(objeto["sinistroid"]))

                    # Removendo índice do comunicado correspondente ao sinistro gravado
                    pipe.srem('ComunicadoSinistro:codigoAgrupador:' + str(codigoAgrupadorRef),
                              str(objeto['numeroprotocolo']))
                    pipe.srem('ComunicadoSinistro:' + str(objeto['numeroprotocolo']) + ':idx',
                              'ComunicadoSinistro:codigoAgrupador:' + str(codigoAgrupadorRef))

                # Removendo índice e comunicado correspondente ao sinistro gravado

                pipe.srem('ComunicadoSinistro:propostaId:' + str(objeto['propostaid']), str(objeto['numeroprotocolo']))
                pipe.srem('ComunicadoSinistro:' + str(objeto['numeroprotocolo']) + ':idx',
                          'ComunicadoSinistro:propostaId:' + str(objeto["propostaid"]))

                pipe.srem('ComunicadoSinistro', str(objeto['numeroprotocolo']))
                pipe.delete('ComunicadoSinistro:' + str(objeto['numeroprotocolo']))

            # Criando os documentos do sinistro baseado nos documentos do comunicado

            documentoComunicadoExiste = str(
                redis.exists('DocumentoComunicadoSinistro:' + str(objeto["numeroprotocolo"])))
            if documentoComunicadoExiste == "1":

                documentoComunicadoHash = redis.hgetall('DocumentoComunicadoSinistro:' + str(objeto["numeroprotocolo"]))
                pipe.sadd('DocumentoSinistro', str(objeto["sinistroid"]))

                pipe.hset('DocumentoSinistro:' + str(objeto["sinistroid"]), 'sinistroId', str(objeto["sinistroid"]))

                for (chave, valor) in documentoComunicadoHash.items():
                    pipe.hset('DocumentoSinistro:' + str(objeto["sinistroid"]), chave, valor)

                # Removendo os documentos do comunicado correspondente ao sinistro gravado

                pipe.srem('DocumentoComunicadoSinistro', str(objeto['numeroprotocolo']))
                pipe.delete('DocumentoComunicadoSinistro:' + str(objeto['numeroprotocolo']))

            # Criando as etapas do sinistro baseadas nas etapas do comunicado

            etapaComunicado = str(redis.exists('comunicado-sinistro-' + str(objeto['numeroprotocolo']) + '-etapas'))
            if etapaComunicado == "1":
                etapaComunicado = redis.get('comunicado-sinistro-' + str(objeto['numeroprotocolo']) + '-etapas')
                etapaComunicado = json.loads(etapaComunicado)

                etapaComunicado['sinistroId'] = str(objeto["sinistroid"])

                # Incluindo o detalhe de numero de protocolo na etapa Aviso registrado
                # O índice da etapa Aviso registrado (= 0) e o mesmo para todas as categorias
                etapaComunicado['etapas'][0]['mesAno'][0]['detalhes'].append(
                    {'id': 3, 'dia': objeto["diaavisosinistro"], 'nome': 'Número de sinistro',
                     'descricao': 'Foi gerado seu número de sinistro: ' + str(objeto["sinistroid"]),
                     'numeroOrdemExibicao': 3})

                # Ordenando os detalhes dentro dos meses/anos
                # O índice da etapa Aviso registrado (= 0) e o mesmo para todas as categorias
                etapaComunicado['etapas'][0]['mesAno'] = weight(etapaComunicado['etapas'][0]['mesAno'])

                pipe.set('sinistro-' + str(objeto["sinistroid"]) + '-etapas', json.dumps(etapaComunicado))

                # Removendo as etapas do comunicado correspondente ao sinistro gravado
                pipe.delete('comunicado-sinistro-' + str(objeto['numeroprotocolo']) + '-etapas')

        if etapaComunicado != "1":

            etapaSinistro = str(redis.exists('sinistro-' + str(objeto['sinistroid']) + '-etapas'))
            if etapaSinistro == "0":
                criar_etapas(objeto)

def salvar_no_redis_sinistro_coberturas(objetos):
    for objeto in objetos:
        logger.info(
            "Gravando a cobertura {} do sinistro {}".format(str(objeto['coberturaid']), str(objeto["sinistroid"])))

        # Criando a cobertura do sinistro

        pipe.sadd('CoberturaSinistro', str(objeto['sinistroid']) + ':' + str(objeto['coberturaid']))
        pipe.hset('CoberturaSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['coberturaid']), 'sinistroId',
                  str(objeto["sinistroid"]))
        pipe.hset('CoberturaSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['coberturaid']), 'coberturaId',
                  str(objeto["coberturaid"]))
        pipe.hset('CoberturaSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['coberturaid']),
                  'valorEstimativa', str(objeto["valorestimativa"]))

        # Associando a cobertura do sinistro ao sinistro

        pipe.hset('Sinistro:' + str(objeto['sinistroid']), 'coberturas.[' + str(objeto["indice"]) + ']',
                  'CoberturaSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['coberturaid']))

def salvar_no_redis_sinistro_historicos(objetos):
    for objeto in objetos:
        logger.info(
            "Gravando o histórico {} do sinistro {}".format(str(objeto['sequencialevento']), str(objeto["sinistroid"])))

        # Criando o histórico do sinistro

        pipe.sadd('HistoricoSinistro', str(objeto['sinistroid']) + ':' + str(objeto['sequencialevento']))
        pipe.hset('HistoricoSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['sequencialevento']),
                  'sinistroId', str(objeto["sinistroid"]))
        pipe.hset('HistoricoSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['sequencialevento']),
                  'sequencialEvento', str(objeto["sequencialevento"]))
        pipe.hset('HistoricoSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['sequencialevento']),
                  'eventoSEGBRId', str(objeto["eventosegbrid"]))
        pipe.hset('HistoricoSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['sequencialevento']),
                  'nomeEventoSEGBR', str(objeto["nomeeventosegbr"]))
        pipe.hset('HistoricoSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['sequencialevento']),
                  'dataEvento', str(objeto["dataevento"]))

        # Associando o histórico do sinistro ao sinistro

        pipe.hset('Sinistro:' + str(objeto['sinistroid']), 'historico.[' + str(objeto["indice"]) + ']',
                  'HistoricoSinistro:' + str(objeto["sinistroid"]) + ':' + str(objeto['sequencialevento']))

def salvar_no_redis_sinistro_estimativa(objetos):
    for objeto in objetos:
        logger.info("Atualizando o valor estimado do sinistro {}".format(str(objeto["sinistroid"])))

        # Atualizando o valor estimado dos sinistros

        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'valorEstimado', str(objeto["valorestimado"]))

def salvar_no_redis_sinistros_pagos(objetos):
    for objeto in objetos:
        logger.info("Atualizando o valor pago do sinistro {}".format(str(objeto["sinistroid"])))

        # Atualizando o valor pago dos sinistros

        pipe.hset('Sinistro:' + str(objeto["sinistroid"]), 'valorPago', str(objeto["valorpago"]))

def criar_etapas(objeto):
    # Criando etapas para o sinistro que não foi aberto pelo mobile
    logger.info("Criando etapas para o sinistro {}".format(str(objeto["sinistroid"])))

    if str(objeto["categoria"]) == "RURAL":
        if str(objeto["numeroprotocolo"]).strip() == "":
            json_string = "{\"numProtocolo\":\"\",\"sinistroId\":" + str(objeto[
                                                                             "sinistroid"]) + ",\"etapas\":[{\"numero\":1,\"nome\":\"Aviso registrado\",\"descricao\":\"\",\"status\":3,\"mesAno\":[{\"nome\":\"" + str(
                objeto["mesextavisosinistro"]) + "\/" + str(
                objeto["anoavisosinistro"]) + "\",\"numeroOrdemExibicao\":1,\"detalhes\":[{\"id\":1,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"Aviso realizado\",\"descricao\":\"\",\"numeroOrdemExibicao\":1},{\"id\":3,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"N\u00famero de sinistro\",\"descricao\":\"Foi gerado seu n\u00famero de sinistro: " + str(
                objeto[
                    "sinistroid"]) + "\",\"numeroOrdemExibicao\":3}]}]},{\"numero\":2,\"nome\":\"Envio da documenta\u00e7\u00e3o\",\"descricao\":\"Confira a documenta\u00e7\u00e3o necess\u00e1ria para iniciarmos a an\u00e1lise do seu processo.\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":3,\"nome\":\"Vistoria\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":4,\"nome\":\"An\u00e1lise\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"Em an\u00e1lise\",\"descricao\":\"O processo est\u00e1 em an\u00e1lise e, em breve, você ser\u00e1 notificado sobre a resposta.\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":5,\"nome\":\"Resultado\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]}]}"
        else:
            json_string = "{\"numProtocolo\":\"" + str(objeto["numeroprotocolo"]) + "\",\"sinistroId\":" + str(objeto[
                                                                                                                   "sinistroid"]) + ",\"etapas\":[{\"numero\":1,\"nome\":\"Aviso registrado\",\"descricao\":\"\",\"status\":3,\"mesAno\":[{\"nome\":\"" + str(
                objeto["mesextavisosinistro"]) + "\/" + str(
                objeto["anoavisosinistro"]) + "\",\"numeroOrdemExibicao\":1,\"detalhes\":[{\"id\":1,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"Aviso realizado\",\"descricao\":\"\",\"numeroOrdemExibicao\":1},{\"id\":2,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"N\u00famero de protocolo\",\"descricao\":\"Foi gerado seu n\u00famero de protocolo: " + str(
                objeto["numeroprotocolo"]) + "\",\"numeroOrdemExibicao\":2},{\"id\":3,\"dia\":" + str(objeto[
                                                                                                          "diaavisosinistro"]) + ",\"nome\":\"N\u00famero de sinistro\",\"descricao\":\"Foi gerado seu n\u00famero de sinistro: " + str(
                objeto[
                    "sinistroid"]) + "\",\"numeroOrdemExibicao\":3}]}]},{\"numero\":2,\"nome\":\"Envio da documenta\u00e7\u00e3o\",\"descricao\":\"Confira a documenta\u00e7\u00e3o necess\u00e1ria para iniciarmos a an\u00e1lise do seu processo.\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":3,\"nome\":\"Vistoria\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":4,\"nome\":\"An\u00e1lise\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"Em an\u00e1lise\",\"descricao\":\"O processo est\u00e1 em an\u00e1lise e, em breve, você ser\u00e1 notificado sobre a resposta.\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":5,\"nome\":\"Resultado\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]}]}"
    else:  # VIDA
        if str(objeto["numeroprotocolo"]).strip() == "":
            json_string = "{\"numProtocolo\":\"\",\"sinistroId\":" + str(objeto[
                                                                             "sinistroid"]) + ",\"etapas\":[{\"numero\":1,\"nome\":\"Aviso registrado\",\"descricao\":\"\",\"status\":3,\"mesAno\":[{\"nome\":\"" + str(
                objeto["mesextavisosinistro"]) + "\/" + str(
                objeto["anoavisosinistro"]) + "\",\"numeroOrdemExibicao\":1,\"detalhes\":[{\"id\":1,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"Aviso realizado\",\"descricao\":\"\",\"numeroOrdemExibicao\":1},{\"id\":3,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"N\u00famero de sinistro\",\"descricao\":\"Foi gerado seu n\u00famero de sinistro: " + str(
                objeto[
                    "sinistroid"]) + "\",\"numeroOrdemExibicao\":3}]}]},{\"numero\":2,\"nome\":\"Envio da documenta\u00e7\u00e3o\",\"descricao\":\"Confira a documenta\u00e7\u00e3o necess\u00e1ria para iniciarmos a an\u00e1lise do seu processo.\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":3,\"nome\":\"An\u00e1lise\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"Em an\u00e1lise\",\"descricao\":\"O processo est\u00e1 em an\u00e1lise e, em breve, você ser\u00e1 notificado sobre a resposta.\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":4,\"nome\":\"Resultado\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]}]}"
        else:
            json_string = "{\"numProtocolo\":\"" + str(objeto["numeroprotocolo"]) + "\",\"sinistroId\":" + str(objeto[
                                                                                                                   "sinistroid"]) + ",\"etapas\":[{\"numero\":1,\"nome\":\"Aviso registrado\",\"descricao\":\"\",\"status\":3,\"mesAno\":[{\"nome\":\"" + str(
                objeto["mesextavisosinistro"]) + "\/" + str(
                objeto["anoavisosinistro"]) + "\",\"numeroOrdemExibicao\":1,\"detalhes\":[{\"id\":1,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"Aviso realizado\",\"descricao\":\"\",\"numeroOrdemExibicao\":1},{\"id\":2,\"dia\":" + str(
                objeto[
                    "diaavisosinistro"]) + ",\"nome\":\"N\u00famero de protocolo\",\"descricao\":\"Foi gerado seu n\u00famero de protocolo: " + str(
                objeto["numeroprotocolo"]) + "\",\"numeroOrdemExibicao\":2},{\"id\":3,\"dia\":" + str(objeto[
                                                                                                          "diaavisosinistro"]) + ",\"nome\":\"N\u00famero de sinistro\",\"descricao\":\"Foi gerado seu n\u00famero de sinistro: " + str(
                objeto[
                    "sinistroid"]) + "\",\"numeroOrdemExibicao\":3}]}]},{\"numero\":2,\"nome\":\"Envio da documenta\u00e7\u00e3o\",\"descricao\":\"Confira a documenta\u00e7\u00e3o necess\u00e1ria para iniciarmos a an\u00e1lise do seu processo.\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":3,\"nome\":\"An\u00e1lise\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"Em an\u00e1lise\",\"descricao\":\"O processo est\u00e1 em an\u00e1lise e, em breve, você ser\u00e1 notificado sobre a resposta.\",\"numeroOrdemExibicao\":0}]}]},{\"numero\":4,\"nome\":\"Resultado\",\"descricao\":\"\",\"status\":0,\"mesAno\":[{\"nome\":\"\",\"numeroOrdemExibicao\":0,\"detalhes\":[{\"id\":0,\"dia\":0,\"nome\":\"\",\"descricao\":\"\",\"numeroOrdemExibicao\":0}]}]}]}"

    item = json.loads(json_string)

    # Ordenando os detalhes dentro dos meses/anos
    # O índice da etapa Aviso registrado (= 0) e o mesmo para todas as categorias
    item['etapas'][0]['mesAno'] = weight(item['etapas'][0]['mesAno'])

    pipe.set("sinistro-" + str(objeto["sinistroid"]) + "-etapas", json.dumps(item))

def salvar_no_redis_documentos_solicitados(objetos):
    sinistro_id = ""
    atualiza_etapa = 0
    todos_concluidos = 1

    for objeto in objetos:

        # Criando a lista de documentos do sinistro

        if sinistro_id != str(objeto["sinistroid"]):

            # Atualizando as etapas do sinistro anterior
            if atualiza_etapa == 1:

                if todos_concluidos == 1:
                    # Atualizando o status da etapa de Envio de documentação para concluído
                    # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
                    item['etapas'][1]['status'] = 3
                else:
                    # Atualizando o status da etapa de Envio de documentação para pendente, caso nao tenha sido
                    # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
                    if item['etapas'][1]['status'] == 0:
                        item['etapas'][1]['status'] = 1

                pipe.set("sinistro-" + sinistro_id + "-etapas", json.dumps(item))

            indice = -1
            sinistro_id = str(objeto["sinistroid"])
            atualiza_etapa = 0
            todos_concluidos = 1

            logger.info("Gravando os documentos solicitados do sinistro {}".format(sinistro_id))

            documentoSinistroExiste = str(redis.exists('DocumentoSinistro:' + sinistro_id))
            if documentoSinistroExiste == "0":
                pipe.sadd('DocumentoSinistro', sinistro_id)
                pipe.hset('DocumentoSinistro:' + sinistro_id, 'sinistroId', str(objeto["sinistroid"]))
                pipe.hset('DocumentoSinistro:' + sinistro_id, 'numeroProtocolo', str(objeto["numeroprotocolo"]))

            etapa = str(redis.exists("sinistro-" + str(objeto["sinistroid"]) + "-etapas"))
            if etapa == "1":
                item = redis.get("sinistro-" + str(objeto["sinistroid"]) + "-etapas")
                item = json.loads(item)
                atualiza_etapa = 1

        indice += 1

        if documentoSinistroExiste == "0":

            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].numeroSolicitacao',
                      str(int(indice) + 1))
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].tabela',
                      str(objeto["tabela"]))
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].idSEGBR',
                      str(objeto["idsegbr"]))
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].nome',
                      str(objeto["nome"]))
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].descricao',
                      str(objeto["descricao"]))
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].numeroOrdemExibicao',
                      str(int(indice) + 1))
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].status',
                      str(objeto["status"]))

            if str(objeto["status"]) == "1":
                todos_concluidos = 0

        else:

            itemDocumentoSinistroExiste = str(
                redis.hexists('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].status'))
            # hexists retorna bolean
            if itemDocumentoSinistroExiste == "True":
                # Atualizando nome e descricao caso exista
                pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].nome',
                          str(objeto["nome"]))
                pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].descricao',
                          str(objeto["descricao"]))
                if str(objeto["status"]) == "4":
                    pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].status',
                              str(objeto["status"]))

    # Atualizando as etapas do sinistro anterior
    if atualiza_etapa == 1:

        if todos_concluidos == 1:
            # Atualizando o status da etapa de Envio de documentação para concluído
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            item['etapas'][1]['status'] = 3
        else:
            # Atualizando o status da etapa de Envio de documentação para pendente, caso nao tenha sido
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            if item['etapas'][1]['status'] == 0:
                item['etapas'][1]['status'] = 1

        pipe.set("sinistro-" + sinistro_id + "-etapas", json.dumps(item))

def salvar_no_redis_documentos_complementar(objetos):
    sinistro_id = ""
    atualiza_etapa = 0
    todos_concluidos = 1
    indice = -1

    for objeto in objetos:

        # Criando a lista de documentos do sinistro

        if sinistro_id != str(objeto["sinistroid"]):

            # Atualizando as etapas do sinistro anterior
            if atualiza_etapa == 1:

                if todos_concluidos == 1:
                    # Atualizando o status da etapa de Envio de documentação para concluído
                    # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
                    item['etapas'][1]['status'] = 3
                else:
                    # Atualizando o status da etapa de Envio de documentação para pendente no cliente
                    # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
                    item['etapas'][1]['status'] = 1

                    # Atualizando o status da etapa de Análise para não executado
                    # Resetando as coleções de meses e detalhes da etapa de Análise
                    if item['etapas'][indiceEtapaAnalise]['status'] != 0:
                        item['etapas'][indiceEtapaAnalise]['status'] = 0
                        item['etapas'][indiceEtapaAnalise]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0,
                                                                         'detalhes': [
                                                                             {'id': 0, 'dia': 0, 'nome': 'Em análise',
                                                                              'descricao': 'O processo está em análise e, em breve, você será notificado sobre a resposta.',
                                                                              'numeroOrdemExibicao': 0}]}]

                    # Atualizando o status da etapa de Resultado para não executado
                    # Resetando as coleções de meses e detalhes da etapa de Resultado
                    if item['etapas'][indiceEtapaResultado]['status'] != 0:
                        item['etapas'][indiceEtapaResultado]['status'] = 0
                        item['etapas'][indiceEtapaResultado]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0,
                                                                           'detalhes': [{'id': 0, 'dia': 0, 'nome': '',
                                                                                         'descricao': '',
                                                                                         'numeroOrdemExibicao': 0}]}]

                pipe.set("sinistro-" + sinistro_id + "-etapas", json.dumps(item))

            sinistro_id = str(objeto["sinistroid"])
            atualiza_etapa = 0
            todos_concluidos = 1

            # Definindo o índice da etapa para cada categoria
            if str(objeto["categoria"]) == "RURAL":
                indiceEtapaAnalise = 3
                indiceEtapaResultado = 4
            else:  # VIDA
                indiceEtapaAnalise = 2
                indiceEtapaResultado = 3

            logger.info("Gravando os documentos complementares do sinistro {}".format(sinistro_id))

            documentoSinistroExiste = str(redis.exists('DocumentoSinistro:' + sinistro_id))
            if documentoSinistroExiste == "0":
                pipe.sadd('DocumentoSinistro', sinistro_id)
                pipe.hset('DocumentoSinistro:' + sinistro_id, 'sinistroId', str(objeto["sinistroid"]))
                pipe.hset('DocumentoSinistro:' + sinistro_id, 'numeroProtocolo', str(objeto["numeroprotocolo"]))
                pipe.execute()

            # Inicializando o índice dos documentos de um sinistro
            # A formula abaixo reflete o padrão de gravação dos documentos
            # Cada documento gravado sempre terá 7 chaves e o hash sempre terá 2 chaves fixas
            indice = redis.hlen('DocumentoSinistro:' + sinistro_id)
            if indice > 0:
                indice = (indice - 2) / 7
                indice = indice - 1
                indice = indice // 1  # garantindo que vai ser um numero inteiro
                adicionadosBasicos = []
                adicionadosComplementares = []
                if indice < 0:
                    indice = 0
                # Definindo o indice caso ja exista doc basico ou complementar adicionados
                documentoSinistroHash = redis.hgetall('DocumentoSinistro:' + sinistro_id)
                for (chave, valor) in documentoSinistroHash.items():
                    # transformando bytes em string com decode
                    chave = chave.decode('utf-8')
                    valor = valor.decode('utf-8')
                    if 'tabela' in chave and 'documento_tb' == valor:
                        adicionadosBasicos.append(chave)
                    if 'tabela' in chave and 'tp_documento_tb' == valor:
                        adicionadosComplementares.append(chave)

                adicionadosBasicos.sort()
                adicionadosComplementares.sort()
                if len(adicionadosBasicos) > 0:
                    # Ultimo documento basico adicionado pra iniciar a partir dele
                    indice_aux = adicionadosBasicos[-1]
                    indice_aux = indice_aux.replace('documentos.[', '').replace('].tabela', '')
                    if indice_aux.isnumeric():
                        indice = int(indice_aux) + 1
                if len(adicionadosComplementares) > 0:
                    # Primeiro documento complementar adicionado pra iniciar dele
                    indice_aux = adicionadosComplementares[0]
                    indice_aux = indice_aux.replace('documentos.[', '').replace('].tabela', '')
                    if indice_aux.isnumeric():
                        indice = int(indice_aux)

            else:
                indice = 0

            etapa = str(redis.exists("sinistro-" + str(objeto["sinistroid"]) + "-etapas"))
            if etapa == "1":
                item = redis.get("sinistro-" + str(objeto["sinistroid"]) + "-etapas")
                item = json.loads(item)
                atualiza_etapa = 1

        status_documento = redis.hget('DocumentoSinistro:' + sinistro_id,
                                      'documentos.[' + str(int(indice)) + '].status')

        pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].numeroSolicitacao',
                  str(int(indice) + 1))
        pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].tabela',
                  str(objeto["tabela"]))
        pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].idSEGBR',
                  str(objeto["idsegbr"]))
        pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].nome', str(objeto["nome"]))
        pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].descricao',
                  str(objeto["descricao"]))
        pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].numeroOrdemExibicao',
                  str(int(indice) + 1))

        # Só vai inserir status caso ainda nao exista o indice ou se status no segbr foi finalizado
        if status_documento == None or str(objeto["status"]) == "4":
            pipe.hset('DocumentoSinistro:' + sinistro_id, 'documentos.[' + str(int(indice)) + '].status',
                      str(objeto["status"]))
            if str(objeto["status"]) == "1":
                todos_concluidos = 0
        else:
            if status_documento.decode('utf-8') == "1":
                todos_concluidos = 0

        indice += 1

    # Atualizando as etapas do sinistro anterior
    if atualiza_etapa == 1:

        if todos_concluidos == 1:
            # Atualizando o status da etapa de Envio de documentação para concluído
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            item['etapas'][1]['status'] = 3
        else:
            # Atualizando o status da etapa de Envio de documentação para pendente no cliente
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            if item['etapas'][1]['status'] != 1:
                item['etapas'][1]['status'] = 1

            # Atualizando o status da etapa de Análise para não executado
            # Resetando as coleções de meses e detalhes da etapa de Análise
            if item['etapas'][indiceEtapaAnalise]['status'] != 0:
                item['etapas'][indiceEtapaAnalise]['status'] = 0
                item['etapas'][indiceEtapaAnalise]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0, 'detalhes': [
                    {'id': 0, 'dia': 0, 'nome': 'Em análise',
                     'descricao': 'O processo está em análise e, em breve, você será notificado sobre a resposta.',
                     'numeroOrdemExibicao': 0}]}]

            # Atualizando o status da etapa de Resultado para não executado
            # Resetando as coleções de meses e detalhes da etapa de Resultado
            if item['etapas'][indiceEtapaResultado]['status'] != 0:
                item['etapas'][indiceEtapaResultado]['status'] = 0
                item['etapas'][indiceEtapaResultado]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0, 'detalhes': [
                    {'id': 0, 'dia': 0, 'nome': '', 'descricao': '', 'numeroOrdemExibicao': 0}]}]

        pipe.set("sinistro-" + sinistro_id + "-etapas", json.dumps(item))

def definir_status_e_detalhes_vistoria_solicitada(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Vistoria solicitada do sinistro {}".format(str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            indiceEtapa = 2  # RURAL

            # Atualizando o status para pendente na seguradora
            item['etapas'][indiceEtapa]['status'] = 2

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>1.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Vistoria solicitada":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>1.2')

                            detalhes['id'] = 1
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Vistoria solicitada"
                            detalhes['descricao'] = ""
                            detalhes['numeroOrdemExibicao'] = 1

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append(
                            {'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Vistoria solicitada', 'descricao': '',
                             'numeroOrdemExibicao': 1})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Vistoria solicitada', 'descricao': '',
                                   'numeroOrdemExibicao': 1}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_vistoria_concluida(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Vistoria concluída do sinistro {}".format(str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            indiceEtapa = 2  # RURAL

            # Atualizando o status para concluído
            item['etapas'][indiceEtapa]['status'] = 3

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Vistoria concluída":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2.2')

                            detalhes['id'] = 3
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Vistoria concluída"
                            detalhes['descricao'] = ""
                            detalhes['numeroOrdemExibicao'] = 3

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append(
                            {'id': 3, 'dia': objeto["dia_evento"], 'nome': 'Vistoria concluída', 'descricao': '',
                             'numeroOrdemExibicao': 3})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': 3, 'dia': objeto["dia_evento"], 'nome': 'Vistoria concluída', 'descricao': '',
                                   'numeroOrdemExibicao': 3}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_vistoria_dispensada(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Vistoria dispensada do sinistro {}".format(str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            indiceEtapa = 2  # RURAL

            # Atualizando o status para concluído
            item['etapas'][indiceEtapa]['status'] = 3

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Vistoria dispensada":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.2')

                            detalhes['id'] = 2
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Vistoria dispensada"
                            detalhes['descricao'] = "Com base em uma análise prévia sua vistoria foi dispensada."
                            detalhes['numeroOrdemExibicao'] = 2

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append({'id': 2, 'dia': objeto["dia_evento"], 'nome': 'Vistoria dispensada',
                                                  'descricao': 'Com base em uma análise prévia sua vistoria foi dispensada.',
                                                  'numeroOrdemExibicao': 2})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': 2, 'dia': objeto["dia_evento"], 'nome': 'Vistoria dispensada',
                                   'descricao': 'Com base em uma análise prévia sua vistoria foi dispensada.',
                                   'numeroOrdemExibicao': 2}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_em_analise_reanalise(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Em análise (por motivo de reanálise ou reabertura) do sinistro {}".format(
                str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            # Atualizando o status da etapa de Envio de documentação para não executado
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            item['etapas'][1]['status'] = 0

            # Definindo o índice da etapa para cada categoria
            if str(objeto["categoria"]) == "RURAL":
                indiceEtapa = 3
                indiceEtapaResultado = 4
            else:  # VIDA
                indiceEtapa = 2
                indiceEtapaResultado = 3

            # Atualizando o status da etapa de Análise para pendente na seguradora
            # Resetando as coleções de meses e detalhes da etapa de Análise
            item['etapas'][indiceEtapa]['status'] = 2
            item['etapas'][indiceEtapa]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0, 'detalhes': [
                {'id': 0, 'dia': 0, 'nome': '', 'descricao': '', 'numeroOrdemExibicao': 0}]}]

            # Atualizando o status da etapa de Resultado para não executado
            # Resetando as coleções de meses e detalhes da etapa de Resultado
            item['etapas'][indiceEtapaResultado]['status'] = 0
            item['etapas'][indiceEtapaResultado]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0, 'detalhes': [
                {'id': 0, 'dia': 0, 'nome': '', 'descricao': '', 'numeroOrdemExibicao': 0}]}]

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Em análise":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2.2')

                            detalhes['id'] = 1
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Em análise"
                            detalhes[
                                'descricao'] = "O processo está em análise novamente e, em breve, você será notificado sobre a resposta."
                            detalhes['numeroOrdemExibicao'] = 1

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append({'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Em análise',
                                                  'descricao': 'O processo está em análise novamente e, em breve, você será notificado sobre a resposta.',
                                                  'numeroOrdemExibicao': 1})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Em análise',
                                   'descricao': 'O processo está em análise novamente e, em breve, você será notificado sobre a resposta.',
                                   'numeroOrdemExibicao': 1}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_em_analise(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Em análise (documentação básica entregue) do sinistro {}".format(
                str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            # Atualizando o status da etapa de Envio de documentação para concluído, caso esteja pendente
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            if item['etapas'][1]['status'] == 1:
                item['etapas'][1]['status'] = 3

            # Definindo o índice da etapa para cada categoria
            if str(objeto["categoria"]) == "RURAL":
                indiceEtapa = 3
            else:  # VIDA
                indiceEtapa = 2

            # Atualizando o status para pendente na seguradora
            # Resetando as coleções de meses e detalhes
            item['etapas'][indiceEtapa]['status'] = 2
            item['etapas'][indiceEtapa]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0, 'detalhes': [
                {'id': 0, 'dia': 0, 'nome': '', 'descricao': '', 'numeroOrdemExibicao': 0}]}]

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Em análise":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3.2')

                            detalhes['id'] = 1
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Em análise"
                            detalhes[
                                'descricao'] = "O processo está em análise e, em breve, você será notificado sobre a resposta."
                            detalhes['numeroOrdemExibicao'] = 1

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append({'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Em análise',
                                                  'descricao': 'O processo está em análise e, em breve, você será notificado sobre a resposta.',
                                                  'numeroOrdemExibicao': 1})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Em análise',
                                   'descricao': 'O processo está em análise e, em breve, você será notificado sobre a resposta.',
                                   'numeroOrdemExibicao': 1}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_analise_finalizada(objetos):
    sinistro_id = ""

    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            if sinistro_id != str(objeto["sinistro_id"]):

                sinistro_id = str(objeto["sinistro_id"])

                logger.info("Gravando a etapa: Análise finalizada do sinistro {}".format(str(objeto["sinistro_id"])))

                item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
                item = json.loads(item)

                # Definindo o índice da etapa para cada categoria
                if str(objeto["categoria"]) == "RURAL":
                    indiceEtapa = 3
                else:  # VIDA
                    indiceEtapa = 2

                # Atualiza caso a etapa de Análise esteja pendente
                if item['etapas'][indiceEtapa]['status'] == 2:

                    # Atualizando o status para concluído
                    item['etapas'][indiceEtapa]['status'] = 3

                    ja_existe_mes = 0
                    for meses in item['etapas'][indiceEtapa]['mesAno']:

                        # Verificando se o mês/ano já existe
                        if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                                objeto["ano_evento"]):
                            ja_existe_mes = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>4.1')

                            meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                            meses['numeroOrdemExibicao'] = 1

                            ja_existe_detalhe = 0
                            for detalhes in meses['detalhes']:

                                # Verificando se o detalhe já existe
                                if detalhes['nome'] == "" or detalhes['nome'] == "Análise finalizada":
                                    ja_existe_detalhe = 1
                                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>4.2')

                                    detalhes['id'] = 2
                                    detalhes['dia'] = objeto["dia_evento"]
                                    detalhes['nome'] = "Análise finalizada"
                                    detalhes['descricao'] = "Concluímos a avaliação do seu aviso."
                                    detalhes['numeroOrdemExibicao'] = 2

                            if ja_existe_detalhe == 0:
                                meses['detalhes'].append(
                                    {'id': 2, 'dia': objeto["dia_evento"], 'nome': 'Análise finalizada',
                                     'descricao': 'Concluímos a avaliação do seu aviso.', 'numeroOrdemExibicao': 2})

                    if ja_existe_mes == 0:
                        item['etapas'][indiceEtapa]['mesAno'].append(
                            {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]),
                             'numeroOrdemExibicao': 1, 'detalhes': [
                                {'id': 2, 'dia': objeto["dia_evento"], 'nome': 'Análise finalizada',
                                 'descricao': 'Concluímos a avaliação do seu aviso.', 'numeroOrdemExibicao': 2}]})

                    # Ordenando os detalhes dentro dos meses/anos
                    item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

                    pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_processo_indeferido(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Processo indeferido do sinistro {}".format(str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            # Atualizando o status da etapa de Envio de documentação para concluído, caso esteja pendente
            # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
            if item['etapas'][1]['status'] == 1:
                item['etapas'][1]['status'] = 3

            # Definindo o índice da etapa para cada categoria
            if str(objeto["categoria"]) == "RURAL":
                indiceEtapa = 4
            else:  # VIDA
                indiceEtapa = 3

            # Atualizando o status para concluído
            item['etapas'][indiceEtapa]['status'] = 3

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>5.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Processo indeferido":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>5.2')

                            detalhes['id'] = 1
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Processo indeferido"
                            detalhes['descricao'] = "Carta de indeferimento."
                            detalhes['numeroOrdemExibicao'] = 1

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append(
                            {'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Processo indeferido', 'descricao': '',
                             'numeroOrdemExibicao': 1})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': 1, 'dia': objeto["dia_evento"], 'nome': 'Processo indeferido', 'descricao': '',
                                   'numeroOrdemExibicao': 1}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

def definir_status_e_detalhes_pagamento_efetuado(objetos):
    sinistro_id = ""

    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            if sinistro_id != str(objeto["sinistro_id"]):

                if sinistro_id != "":
                    # Ordenando os detalhes dentro dos meses/anos
                    item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

                    pipe.set("sinistro-" + sinistro_id + "-etapas", json.dumps(item))

                sinistro_id = str(objeto["sinistro_id"])
                indice = 1

                logger.info("Gravando a etapa: Pagamento efetuado do sinistro {}".format(sinistro_id))

                item = redis.get("sinistro-" + sinistro_id + "-etapas")
                item = json.loads(item)

                # Atualizando o status da etapa de Envio de documentação para concluído, caso esteja pendente
                # O índice da etapa Envio de documentação (= 1) é o mesmo para todas as categorias
                if item['etapas'][1]['status'] == 1:
                    item['etapas'][1]['status'] = 3

                # Definindo o índice da etapa para cada categoria
                if str(objeto["categoria"]) == "RURAL":
                    indiceEtapa = 4
                else:  # VIDA
                    indiceEtapa = 3

                # Atualizando o status para concluído
                # Resetando as coleções de meses e detalhes
                item['etapas'][indiceEtapa]['status'] = 3
                item['etapas'][indiceEtapa]['mesAno'] = [{'nome': '', 'numeroOrdemExibicao': 0, 'detalhes': []}]

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>6.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1
                    meses['detalhes'].append({'id': indice, 'dia': objeto["dia_evento"], 'nome': 'Pagamento efetuado',
                                              'descricao': ('Valor deferido R$%0.2f' % (objeto["val_pago"])).replace(
                                                  ".", ",") + '.', 'numeroOrdemExibicao': indice})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [{'id': indice, 'dia': objeto["dia_evento"], 'nome': 'Pagamento efetuado',
                                   'descricao': ('Valor deferido R$%0.2f' % (objeto["val_pago"])).replace(".",
                                                                                                          ",") + '.',
                                   'numeroOrdemExibicao': indice}]})

            indice += 1

    if sinistro_id != "":
        # Ordenando os detalhes dentro dos meses/anos
        item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

        pipe.set("sinistro-" + sinistro_id + "-etapas", json.dumps(item))

def definir_status_e_detalhes_aviso_finalizado(objetos):
    for objeto in objetos:

        etapa = str(redis.exists("sinistro-" + str(objeto["sinistro_id"]) + "-etapas"))
        if etapa == "1":

            logger.info("Gravando a etapa: Aviso finalizado do sinistro {}".format(str(objeto["sinistro_id"])))

            item = redis.get("sinistro-" + str(objeto["sinistro_id"]) + "-etapas")
            item = json.loads(item)

            # Definindo o índice da etapa para cada categoria
            if str(objeto["categoria"]) == "RURAL":
                indiceEtapa = 4
            else:  # VIDA
                indiceEtapa = 3

            # Atualizando o status para concluído (aviso finalizado)
            item['etapas'][indiceEtapa]['status'] = 4

            ja_existe_mes = 0
            for meses in item['etapas'][indiceEtapa]['mesAno']:

                # Verificando se o mês/ano já existe
                if meses['nome'] == "" or meses['nome'] == str(objeto["mes_ext_evento"]) + "/" + str(
                        objeto["ano_evento"]):
                    ja_existe_mes = 1
                    print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>7.1')

                    meses['nome'] = str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"])
                    meses['numeroOrdemExibicao'] = 1

                    ja_existe_detalhe = 0
                    for detalhes in meses['detalhes']:

                        # Verificando se o detalhe já existe
                        if detalhes['nome'] == "" or detalhes['nome'] == "Aviso finalizado":
                            ja_existe_detalhe = 1
                            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>7.2')

                            detalhes['id'] = objeto["qtd_eventos"] + 1
                            detalhes['dia'] = objeto["dia_evento"]
                            detalhes['nome'] = "Aviso finalizado"
                            detalhes['descricao'] = ""
                            detalhes['numeroOrdemExibicao'] = objeto["qtd_eventos"] + 1

                    if ja_existe_detalhe == 0:
                        meses['detalhes'].append(
                            {'id': objeto["qtd_eventos"] + 1, 'dia': objeto["dia_evento"], 'nome': 'Aviso finalizado',
                             'descricao': '', 'numeroOrdemExibicao': objeto["qtd_eventos"] + 1})

            if ja_existe_mes == 0:
                item['etapas'][indiceEtapa]['mesAno'].append(
                    {'nome': str(objeto["mes_ext_evento"]) + "/" + str(objeto["ano_evento"]), 'numeroOrdemExibicao': 1,
                     'detalhes': [
                         {'id': objeto["qtd_eventos"] + 1, 'dia': objeto["dia_evento"], 'nome': 'Aviso finalizado',
                          'descricao': '', 'numeroOrdemExibicao': objeto["qtd_eventos"] + 1}]})

            # Ordenando os detalhes dentro dos meses/anos
            item['etapas'][indiceEtapa]['mesAno'] = weight(item['etapas'][indiceEtapa]['mesAno'])

            pipe.set("sinistro-" + str(objeto["sinistro_id"]) + "-etapas", json.dumps(item))

# Início da execução

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'redis_host', 'redis_port', 'redis_db', 'redis_ssl', 'redis_auth',
                                     'redshift_key_path', 'type', 'redshift_credentials'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

sqlctx = SQLContext(sc)
logger = glueContext.get_logger()

job.init(args['JOB_NAME'], args)

s3 = boto3.resource("s3")

logger.info("Recuperando os parâmetros do Redis")

ssl = args['redis_ssl']

if ssl:
	redis = redis.Redis(host=get_aws_sm_parameter(args['redis_host']),
		port=get_aws_sm_parameter(args['redis_port']),
		db=args['redis_db'],
		ssl=args['redis_ssl'],
		password=get_aws_sm_parameter(args['redis_auth']))
else:
	redis = redis.Redis(host=get_aws_sm_parameter(args['redis_host']),
		port=get_aws_sm_parameter(args['redis_port']),
		db=args['redis_db'])

pipe = redis.pipeline()

# Recuperando os parâmetros do Redshift
logger.info("Recuperando os parâmetros do Redshift")

redshiftConfig = get_aws_sm_value(args['redshift_credentials'], "us-east-1")

logger.info("Selecionado sinistros de propostas vigentes - Vida e Rural - AB")

# Selecionando sinistros de proposta individual (apolice) vida

query_sinistros_vigencia = """select p.proposta_id,
      p.produto_id,
      'VIDA' as categoria,
      s.sinistro_id,
      s.nr_ptc_aviso,
      1 as cod_objeto_segurado,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      from spectrum_seguros_db_ab_mobile.proposta_tb p
      join spectrum_seguros_db_ab_mobile.apolice_tb a
      on a.proposta_id = p.proposta_id
      left join spectrum_seguros_db_ab_mobile.endosso_tb e
      on e.proposta_id = p.proposta_id
      and e.tp_endosso_id = 314
      join spectrum_seguros_db_ab_mobile.sinistro_tb s
      on s.proposta_id = p.proposta_id
      where p.produto_id in (1217, 1218, 1235, 1236, 1237)
      and p.situacao = 'i'
      and a.dt_inicio_vigencia <= getdate()
      and s.evento_sinistro_id in (103, 298, 428)
      group by p.proposta_id,
      p.produto_id,
      s.sinistro_id,
      s.nr_ptc_aviso,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      having max(isnull(e.dt_fim_vigencia_end, a.dt_fim_vigencia) + 1) > getdate()"""

table_temp_ids = "temp_pk_tabela_ab_mobile.chaves_redis_sinistro"

if args['type'] == 'increment':
    query_sinistros_vigencia += " and s.sinistro_id in (select sinistro_id from {} group by sinistro_id)".format(table_temp_ids)

# Selecionando sinistro de proposta individual (apolice) rural

query_sinistros_vigencia += """ union
      select p.proposta_id,
      p.produto_id,
      'RURAL' as categoria,
      s.sinistro_id,
      s.nr_ptc_aviso,
      1 as cod_objeto_segurado,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      from spectrum_seguros_db_ab_mobile.proposta_tb p
      join spectrum_seguros_db_ab_mobile.apolice_tb a
      on a.proposta_id = p.proposta_id
      join spectrum_seguros_db_ab_mobile.sinistro_tb s
      on s.proposta_id = p.proposta_id
      where p.produto_id in (1152, 1204)
      and p.situacao = 'i'
      and a.dt_inicio_vigencia <= getdate()
      and a.dt_fim_vigencia + 1 > getdate()"""

if args['type'] == 'increment':
    query_sinistros_vigencia += " and s.sinistro_id in (select sinistro_id from {} group by sinistro_id)".format(table_temp_ids)

# Selecionando sinistro de proposta adesão (apolice) vida

query_sinistros_vigencia += """ union
      select p.proposta_id,
      p.produto_id,
      'VIDA' as categoria,
      s.sinistro_id,
      s.nr_ptc_aviso,
      1 as cod_objeto_segurado,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      from spectrum_seguros_db_ab_mobile.proposta_tb p
      join spectrum_seguros_db_ab_mobile.proposta_adesao_tb a
      on a.proposta_id = p.proposta_id
      left join spectrum_seguros_db_ab_mobile.endosso_tb e
      on e.proposta_id = p.proposta_id
      and e.tp_endosso_id in (314, 250)
      join spectrum_seguros_db_ab_mobile.sinistro_tb s
      on s.proposta_id = p.proposta_id
      where p.produto_id in (721, 1174, 1177, 1179, 1180, 1181, 1182, 1196)
      and p.situacao = 'i'
      and a.dt_inicio_vigencia <= getdate()
      and a.dt_fim_vigencia is not null
      and s.evento_sinistro_id in (103, 298, 428)
      group by p.proposta_id,
      p.produto_id,
      s.sinistro_id,
      s.nr_ptc_aviso,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      having max(isnull(e.dt_fim_vigencia_end, a.dt_fim_vigencia) + 1) > getdate()"""

if args['type'] == 'increment':
    query_sinistros_vigencia += " and s.sinistro_id in (select sinistro_id from {} group by sinistro_id)".format(table_temp_ids)

# Selecionando sinistros de proposta adesão (apolice) rural

query_sinistros_vigencia += """ union
      select p.proposta_id,
      p.produto_id,
      'RURAL' as categoria,
      s.sinistro_id,
      s.nr_ptc_aviso,
      1 as cod_objeto_segurado,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      from spectrum_seguros_db_ab_mobile.proposta_tb p
      join spectrum_seguros_db_ab_mobile.proposta_adesao_tb a
      on a.proposta_id = p.proposta_id
      join spectrum_seguros_db_ab_mobile.sinistro_tb s
      on s.proposta_id = p.proposta_id
      where p.produto_id in (155)
      and p.situacao = 'i'
      and a.dt_inicio_vigencia <= getdate()
      and a.dt_fim_vigencia + 1 > getdate()"""

if args['type'] == 'increment':
    query_sinistros_vigencia += " and s.sinistro_id in (select sinistro_id from {} group by sinistro_id)".format(table_temp_ids)

# Selecionando sinistros de proposta adesão (apolice) vida II

query_sinistros_vigencia += """ union
      select p.proposta_id,
      p.produto_id,
      'VIDA' as categoria,
      s.sinistro_id,
      s.nr_ptc_aviso,
      1 as cod_objeto_segurado,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      from spectrum_seguros_db_ab_mobile.proposta_tb p
      join spectrum_seguros_db_ab_mobile.proposta_adesao_tb a
      on a.proposta_id = p.proposta_id
      left join spectrum_seguros_db_ab_mobile.endosso_tb e
      on e.proposta_id = p.proposta_id
      and e.tp_endosso_id = 202
      join spectrum_seguros_db_ab_mobile.sinistro_tb s
      on s.proposta_id = p.proposta_id
      where p.produto_id in (121, 135, 136, 716)
      and p.situacao = 'i'
      and a.dt_inicio_vigencia <= getdate()
      and s.evento_sinistro_id in (103, 298, 428)
      group by p.proposta_id,
      p.produto_id,
      s.sinistro_id,
      s.nr_ptc_aviso,
      s.evento_sinistro_id,
      s.dt_aviso_sinistro,
      s.dt_ocorrencia_sinistro,
      s.situacao,
      s.solicitante_id
      having max(dateadd(year, 1, isnull(e.dt_pedido_endosso, a.dt_inicio_vigencia)) + 1) > getdate()"""

if args['type'] == 'increment':
    query_sinistros_vigencia += " and s.sinistro_id in (select sinistro_id from {} group by sinistro_id)".format(table_temp_ids)

resultado_sinistros_vigencia = query_to_data_frame(query_sinistros_vigencia)

write_redshift(resultado_sinistros_vigencia, "stage_sinistro_vigencia_ab")

# Selecionando os sinistros a serem gravados
logger.info("Selecionando os sinistros a serem gravados")

query_sinistros = """select sin.sinistro_id as sinistroid,
    isnull(sin.nr_ptc_aviso, ' ') as numeroProtocolo,
    sin.proposta_id as propostaId,
    sin.cod_objeto_segurado as codObjetoSegurado,
    sin.evento_sinistro_id as eventoSinistroId,
    sin.dt_aviso_sinistro as dataAbertura,
    sin.dt_ocorrencia_sinistro as dataOcorrencia,
    sin.situacao,
    sin.categoria,
    isnull(sinbb.sinistro_bb, 0) as sinistroBancoBrasil,  
    0 as valorEstimado,
    0 as valorPago,
    solic.solicitante_id as solicitanteid,
    solic.nome as solicitantenome,
    isnull(solic.endereco, ' ') as endereco,
    isnull(solic.bairro, ' ') as bairro,
    isnull(solic.municipio, ' ') as municipio,
    isnull(solic.estado, ' ') as estado,
    isnull(solic.cep, ' ') as cep,
    isnull(solic.ddd, ' ') as dddTelefone,
    isnull(solic.telefone, ' ') as telefone,
    isnull(solic.tp_telefone, ' ') as tipoTelefone,
    isnull(solic.email, ' ') as email,
    isnull(sinvi.nome, ' ') as nome,
    isnull(sinvi.cpf, ' ') as cpf,
    isnull(cast(sinvi.dt_nascimento as varchar), ' ') as dtnascimento,
    isnull(sinvi.sexo, ' ') as sexo,
    cast(dataAbertura as date) as data_aviso_sinistro,
    cast(date_part(day, data_aviso_sinistro) as integer) as diaAvisoSinistro,
    cast(date_part(month, data_aviso_sinistro) as integer) as mesAvisoSinistro,
    case mesAvisoSinistro
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mesExtAvisoSinistro,
    cast(date_part(year, data_aviso_sinistro) as integer) as anoAvisoSinistro
    from stage_sinistro_vigencia_ab sin
    left join spectrum_seguros_db_ab_mobile.sinistro_vida_tb sinvi
    on sin.sinistro_id = sinvi.sinistro_id
    left join (select sinbb2.sinistro_id, max(sinbb2.dt_inicio_vigencia) as dt_inicio_vigencia
               from spectrum_seguros_db_ab_mobile.sinistro_bb_tb sinbb2
               where sinbb2.dt_fim_vigencia is null
               group by sinbb2.sinistro_id) sinbbmax
    on sin.sinistro_id = sinbbmax.sinistro_id
    left join spectrum_seguros_db_ab_mobile.sinistro_bb_tb sinbb
    on sinbbmax.sinistro_id = sinbb.sinistro_id
    and sinbbmax.dt_inicio_vigencia = sinbb.dt_inicio_vigencia
    and sinbb.dt_fim_vigencia is null
    join spectrum_seguros_db_ab_mobile.solicitante_sinistro_tb solic
    on sin.solicitante_id = solic.solicitante_id"""

resultado_sinistros = query_to_data_frame(query_sinistros)

enviar_sinistros(resultado_sinistros.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando as coberturas de sinistro a serem gravadas
logger.info("Selecionando as coberturas de sinistros a serem gravadas")

query_sinistro_coberturas = """select sincob.sinistro_id as sinistroId,
    sincob.tp_cobertura_id as coberturaId,
    sincob.val_estimativa as valorEstimativa,
    (row_number() over (partition by sincob.sinistro_id order by sincob.tp_cobertura_id)) - 1 as indice
    from stage_sinistro_vigencia_ab sin
    join spectrum_seguros_db_ab_mobile.sinistro_cobertura_tb sincob
    on sin.sinistro_id = sincob.sinistro_id
    and sincob.dt_fim_vigencia is null"""

resultado_sinistro_coberturas = query_to_data_frame(query_sinistro_coberturas)

enviar_sinistro_coberturas(resultado_sinistro_coberturas.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os históricos de sinistro a serem gravados
logger.info("Selecionando os históricos de sinistros a serem gravados")

query_sinistro_historicos = """select sinhist.sinistro_id as sinistroId,
    sinhist.seq_evento as sequencialEvento,
    sinhist.evento_SEGBR_id as eventoSEGBRId,
    eve.descricao as nomeEventoSEGBR,
    sinhist.dt_evento as dataEvento,
    seq_evento - 1 as indice
    from stage_sinistro_vigencia_ab sin
    join spectrum_seguros_db_ab_mobile.sinistro_historico_tb sinhist
    on sin.sinistro_id = sinhist.sinistro_id
    join spectrum_seguros_db_ab_mobile.evento_segbr_tb eve
    on sinhist.evento_segbr_id = eve.evento_segbr_id"""

resultado_sinistro_historicos = query_to_data_frame(query_sinistro_historicos)

enviar_sinistro_historicos(resultado_sinistro_historicos.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando as estimativas a serem atualizadas nos sinistros
logger.info("Selecionando as estimativas a serem atualizadas nos sinistros")

query_sinistro_estimativa = """select sinest.sinistro_id as sinistroId,
    sinest.val_estimado as valorEstimado
    from stage_sinistro_vigencia_ab sin
    join spectrum_seguros_db_ab_mobile.sinistro_estimativa_tb sinest
    on sin.sinistro_id = sinest.sinistro_id
    and sinest.item_val_estimativa = 1
    and sinest.dt_fim_estimativa is null"""

resultado_sinistro_estimativa = query_to_data_frame(query_sinistro_estimativa)

enviar_sinistro_estimativa(resultado_sinistro_estimativa.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os pagamentos a serem atualizados nos sinistros
logger.info("Selecionando os pagamentos a serem atualizadas nos sinistros")

query_sinistros_pagos = """select sinpag.sinistro_id as sinistroId,
    sum(sinpag.val_acerto + sinpag.val_correcao) as valorPago
    from stage_sinistro_vigencia_ab sin
    join spectrum_seguros_db_ab_mobile.pgto_sinistro_tb sinpag
    on sin.sinistro_id = sinpag.sinistro_id
    and sinpag.situacao_op = 'a'
    and sinpag.item_val_estimativa = 1
    group by sinpag.sinistro_id"""

resultado_sinistros_pagos = query_to_data_frame(query_sinistros_pagos)

enviar_sinistros_pagos(resultado_sinistros_pagos.toJSON().map(lambda x: json.loads(x)).collect())

# Controle de status das etapas
# Obs.: Respeitar a ordem de execução das queries abaixo, é primordial para a correta gravação das etapas

# Selecionando os sinistros com status: Em análise (por motivo de reanálise ou reabertura)
logger.info("Selecionando os sinistros com status: Em análise (por motivo de reanálise ou reabertura)")

query_em_analise_reanalise = """select essa1.sinistro_id,
    essa1.categoria,
    max(essa1.dt_evento) as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          sin.categoria,
          cast(essa.dt_evento as date) as dt_evento
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on essa.sinistro_id = sin.sinistro_id
          where essa.evento_id = (select max(essa2.evento_id)
                                  from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                                  where essa2.sinistro_id = essa.sinistro_id
                                  and essa2.evento_bb_id = 1190)
          union
          select essa.sinistro_id,
          sin.categoria,
          max(cast(essa.dt_evento as date)) as dt_evento
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on essa.sinistro_id = sin.sinistro_id
          where essa.evento_bb_id = 1100
          group by essa.sinistro_id,
          sin.categoria
          having count(essa.sinistro_id) > 1) essa1
    group by essa1.sinistro_id,
    essa1.categoria"""

resultado_em_analise_reanalise = query_to_data_frame(query_em_analise_reanalise)

enviar_em_analise_reanalise(resultado_em_analise_reanalise.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando a documentação básica dos sinistros a serem gravados
logger.info("Selecionando a documentação básica dos sinistros a serem gravados")

query_documentos_solicitados = """select essa1.sinistro_id as sinistroId,
    isnull(essa1.nr_ptc_aviso, ' ') as numeroProtocolo,
    essa1.categoria,
    essa1.evento_id as eventoId,
    'documento_tb' as tabela,
    doc.documento_id as idsegbr,
    isnull(doc.nome_exibicao, ' ') as nome,
    isnull(doc.descricao_exibicao, ' ') as descricao,
    case when (select count(essa2.evento_id)
               from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
               where essa2.sinistro_id = essa1.sinistro_id
               and essa2.evento_id > essa1.evento_id
               and essa2.evento_bb_id = 1110) > 0
    then 4
    else 1
    end as status
    from (select essa.sinistro_id,
          essa.nr_ptc_aviso,
          sin.categoria,
          min(essa.evento_id) as evento_id
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on essa.sinistro_id = sin.sinistro_id
          where essa.evento_bb_id = 1100
          group by essa.sinistro_id,
          essa.nr_ptc_aviso,
          sin.categoria) essa1
    join spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_documento_tb essd
    on essa1.evento_id = essd.evento_id
    left join spectrum_seguros_db_ab_mobile.documento_tb doc
    on doc.documento_id = essd.documento_id
    order by essa1.sinistro_id,
    essd.documento_id"""

resultado_documentos_solicitados = query_to_data_frame(query_documentos_solicitados)

enviar_documentos_solicitados(resultado_documentos_solicitados.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando a documentação complementar dos sinistros a serem gravados
logger.info("Selecionando a documentação complementar dos sinistros a serem gravados")

query_documentos_complementar = """select exidoc.sinistro_id as sinistroId,
    isnull(sin.nr_ptc_aviso, ' ') as numeroProtocolo,
    sin.categoria,
    exidoc.seq_exigencia as exigenciaId,
    'tp_documento_tb' as tabela,
    exidoc.documento_id as idsegbr,
    isnull(tpdoc.nome_exibicao, ' ') as nome,
    isnull(tpdoc.descricao_exibicao, ' ') as descricao,
    case when exi.dt_atendida is not null
    then 4
    else 1
    end as status
    from stage_sinistro_vigencia_ab sin
    join spectrum_seguros_db_ab_mobile.exigencia_tb exi
    on exi.sinistro_id = sin.sinistro_id
    join spectrum_seguros_db_ab_mobile.exigencia_documento_tb exidoc
    on exidoc.sinistro_id = exi.sinistro_id
    and exidoc.seq_exigencia = exi.seq_exigencia
    join spectrum_seguros_db_ab_mobile.tp_documento_tb tpdoc
    on tpdoc.tp_documento_id = exidoc.documento_id
    where not (exidoc.seq_exigencia = 1 and exidoc.documento_id = 1076)
    order by exidoc.sinistro_id,
    exidoc.seq_exigencia,
    exidoc.documento_id"""

resultado_documentos_complementar = query_to_data_frame(query_documentos_complementar)

enviar_documentos_complementar(resultado_documentos_complementar.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Vistoria solicitada
logger.info("Selecionando os sinistros com status: Vistoria solicitada")

query_status_vistoria_solicitada = """select vis1.sinistro_id,
    vis1.dt_pedido_vistoria as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when '1' then 'JANEIRO'
    when '2' then 'FEVEREIRO'
    when '3' then 'MARÇO'
    when '4' then 'ABRIL'
    when '5' then 'MAIO'
    when '6' then 'JUNHO'
    when '7' then 'JULHO'
    when '8' then 'AGOSTO'
    when '9' then 'SETEMBRO'
    when '10' then 'OUTUBRO'
    when '11' then 'NOVEMBRO'
    when '12' then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select vis.sinistro_id,
          cast(vis.dt_pedido_vistoria as date) as dt_pedido_vistoria
          from stage_sinistro_vigencia_ab sin
          join spectrum_seguros_db_ab_mobile.vistoria_tb vis
          on sin.sinistro_id = vis.sinistro_id
          and vis.dt_pedido_vistoria = (select max(vis2.dt_pedido_vistoria)
                                        from spectrum_seguros_db_ab_mobile.vistoria_tb vis2
                                        where vis2.sinistro_id = vis.sinistro_id)
          where sin.categoria = 'RURAL') vis1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
                      where essa.sinistro_id = vis1.sinistro_id
                      and cast(essa.dt_evento as date) > cast(vis1.dt_pedido_vistoria as date)
                      and essa.evento_bb_id in (1190, 1100))"""

resultado_vistoria_solicitada = query_to_data_frame(query_status_vistoria_solicitada)

enviar_vistoria_solicitada(resultado_vistoria_solicitada.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Vistoria concluída
logger.info("Selecionando os sinistros com status: Vistoria concluída")

query_status_vistoria_concluida = """select vis1.sinistro_id,
    vis1.dt_parecer_vistoria as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when '1' then 'JANEIRO'
    when '2' then 'FEVEREIRO'
    when '3' then 'MARÇO'
    when '4' then 'ABRIL'
    when '5' then 'MAIO'
    when '6' then 'JUNHO'
    when '7' then 'JULHO'
    when '8' then 'AGOSTO'
    when '9' then 'SETEMBRO'
    when '10' then 'OUTUBRO'
    when '11' then 'NOVEMBRO'
    when '12' then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select vis.sinistro_id,
          cast(vis.dt_pedido_vistoria as date) as dt_pedido_vistoria,
          cast(vis.dt_parecer_vistoria as date) as dt_parecer_vistoria
          from stage_sinistro_vigencia_ab sin
          join spectrum_seguros_db_ab_mobile.vistoria_tb vis
          on sin.sinistro_id = vis.sinistro_id
          and vis.dt_pedido_vistoria = (select max(vis2.dt_pedido_vistoria)
                                        from spectrum_seguros_db_ab_mobile.vistoria_tb vis2
                                        where vis2.sinistro_id = vis.sinistro_id)
          and vis.dt_parecer_vistoria is not null
          where sin.categoria = 'RURAL') vis1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
                      where essa.sinistro_id = vis1.sinistro_id
                      and cast(essa.dt_evento as date) > cast(vis1.dt_pedido_vistoria as date)
                      and essa.evento_bb_id in (1190, 1100))"""

resultado_vistoria_concluida = query_to_data_frame(query_status_vistoria_concluida)

enviar_vistoria_concluida(resultado_vistoria_concluida.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Vistoria dispensada
logger.info("Selecionando os sinistros com status: Vistoria dispensada")

query_status_vistoria_dispensada = """select essa1.sinistro_id,
    max(essa1.data_efetivacao) as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when '1' then 'JANEIRO'
    when '2' then 'FEVEREIRO'
    when '3' then 'MARÇO'
    when '4' then 'ABRIL'
    when '5' then 'MAIO'
    when '6' then 'JUNHO'
    when '7' then 'JULHO'
    when '8' then 'AGOSTO'
    when '9' then 'SETEMBRO'
    when '10' then 'OUTUBRO'
    when '11' then 'NOVEMBRO'
    when '12' then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          essa.num_recibo,
          essa.evento_id,
          cast(pgto.dt_recebimento_cliente as date) as data_efetivacao
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on sin.sinistro_id = essa.sinistro_id
          join spectrum_seguros_db_ab_mobile.pgto_sinistro_tb pgto
          on pgto.sinistro_id = essa.sinistro_id
          and pgto.num_recibo = essa.num_recibo
          and pgto.situacao_op = 'a'
          and pgto.item_val_estimativa = 1
          and pgto.dt_recebimento_cliente is not null
          join spectrum_seguros_db_ab_mobile.ps_acerto_pagamento_tb psa
          on psa.acerto_id = pgto.acerto_id
          and psa.voucher_id is not null
          where essa.evento_bb_id = 1152
          and sin.categoria = 'RURAL') essa1
    join spectrum_seguros_db_ab_mobile.vistoria_tb vis
    on vis.sinistro_id = essa1.sinistro_id
    and vis.dt_pedido_vistoria <= essa1.data_efetivacao
    and vis.dt_pedido_vistoria = (select max(vis2.dt_pedido_vistoria)
                                from spectrum_seguros_db_ab_mobile.vistoria_tb vis2
                                where vis2.sinistro_id = vis.sinistro_id)
    and vis.dt_parecer_vistoria is null
    where not exists (select 1
                    from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                    where essa2.sinistro_id = essa1.sinistro_id
                    and essa2.num_recibo = essa1.num_recibo
                    and essa2.evento_bb_id in (1154, 1156))
    and not exists (select 1
                    from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                    where essa3.sinistro_id = essa1.sinistro_id
                    and essa3.evento_id > essa1.evento_id
                    and essa3.evento_bb_id in (1190, 1100))
    group by essa1.sinistro_id
    union
    select essa1.sinistro_id,
    essa1.dt_evento as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when '1' then 'JANEIRO'
    when '2' then 'FEVEREIRO'
    when '3' then 'MARÇO'
    when '4' then 'ABRIL'
    when '5' then 'MAIO'
    when '6' then 'JUNHO'
    when '7' then 'JULHO'
    when '8' then 'AGOSTO'
    when '9' then 'SETEMBRO'
    when '10' then 'OUTUBRO'
    when '11' then 'NOVEMBRO'
    when '12' then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          essa.evento_id,
          cast(essa.dt_evento as date) as dt_evento
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on sin.sinistro_id = essa.sinistro_id
          join spectrum_seguros_db_ab_mobile.vistoria_tb vis
          on vis.sinistro_id = essa.sinistro_id
          and vis.dt_pedido_vistoria <= essa.dt_evento
          and vis.dt_pedido_vistoria = (select max(vis2.dt_pedido_vistoria)
                                        from spectrum_seguros_db_ab_mobile.vistoria_tb vis2
                                        where vis2.sinistro_id = vis.sinistro_id)
          and vis.dt_parecer_vistoria is null
          where essa.evento_id = (select max(essa2.evento_id)
                                  from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                                  where essa2.sinistro_id = essa.sinistro_id
                                  and essa2.evento_bb_id = 1140)
          and sin.categoria = 'RURAL') essa1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                      where essa3.sinistro_id = essa1.sinistro_id
                      and essa3.evento_id > essa1.evento_id
                      and essa3.evento_bb_id in (1190, 1100))"""

resultado_vistoria_dispensada = query_to_data_frame(query_status_vistoria_dispensada)

enviar_vistoria_dispensada(resultado_vistoria_dispensada.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Em análise (documentação básica entregue)
logger.info("Selecionando os sinistros com status: Em análise (documentação básica entregue)")

query_em_analise = """select essa1.sinistro_id,
    essa1.categoria,
    min(cast(essa2.dt_evento as date)) as data_evento,
    cast(datepart(day, data_evento) as integer) as dia_evento,
    cast(datepart(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(datepart(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          sin.categoria,
          min(essa.evento_id) as evento_id
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on essa.sinistro_id = sin.sinistro_id
          where essa.evento_bb_id = 1100
          group by essa.sinistro_id,
          sin.categoria) essa1
    join spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
    on essa2.sinistro_id = essa1.sinistro_id
    and essa2.evento_id > essa1.evento_id
    and essa2.evento_bb_id = 1110
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                      where essa3.sinistro_id = essa1.sinistro_id
                      and essa3.evento_id > essa1.evento_id
                      and essa3.evento_bb_id in (1190, 1100, 1130))
    group by essa1.sinistro_id,
    essa1.categoria"""

resultado_em_analise = query_to_data_frame(query_em_analise)

enviar_em_analise(resultado_em_analise.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Análise finalizada / Processo indeferido
logger.info("Selecionando os sinistros com status: Análise finalizada / Processo indeferido")

query_status_processo_indeferido = """select essa1.sinistro_id,
    essa1.categoria,
    essa1.dt_evento as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          essa.evento_id,
          sin.categoria,
          cast(essa.dt_evento as date) as dt_evento
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on essa.sinistro_id = sin.sinistro_id
          where essa.evento_id = (select max(essa2.evento_id)
                                  from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                                  where essa2.sinistro_id = essa.sinistro_id
                                  and essa2.evento_bb_id = 1140)) essa1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                      where essa3.sinistro_id = essa1.sinistro_id
                      and essa3.evento_id > essa1.evento_id
                      and essa3.evento_bb_id in (1190, 1100))"""

resultado_processo_indeferido = query_to_data_frame(query_status_processo_indeferido)

enviar_analise_finalizada(resultado_processo_indeferido.toJSON().map(lambda x: json.loads(x)).collect())
enviar_processo_indeferido(resultado_processo_indeferido.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Análise finalizada / Pagamento efetuado
logger.info("Selecionando os sinistros com status: Análise finalizada / Pagamento efetuado")

query_status_pagamento_efetuado = """select essa1.sinistro_id,
    essa1.categoria,
    essa1.val_pago,
    essa1.dt_recebimento_cliente as data_efetivacao,
    cast(date_part(day, data_efetivacao) as integer) as dia_evento,
    cast(date_part(month, data_efetivacao) as integer) as mes_evento,
    case mes_evento
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_efetivacao) as integer) as ano_evento
    from (select essa.sinistro_id,
          sin.categoria,
          essa.num_recibo,
          essa.evento_id,
          pgto.val_acerto + isnull(pgto.val_correcao, 0) as val_pago,
          cast(pgto.dt_recebimento_cliente as date) as dt_recebimento_cliente
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on essa.sinistro_id = sin.sinistro_id
          join spectrum_seguros_db_ab_mobile.pgto_sinistro_tb pgto
          on pgto.sinistro_id = essa.sinistro_id
          and pgto.num_recibo = essa.num_recibo
          and pgto.situacao_op = 'a'
          and pgto.item_val_estimativa = 1
          and pgto.dt_recebimento_cliente is not null
          join spectrum_seguros_db_ab_mobile.ps_acerto_pagamento_tb psa
          on psa.acerto_id = pgto.acerto_id
          and psa.voucher_id is not null
          where essa.evento_bb_id = 1152) essa1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                      where essa2.sinistro_id = essa1.sinistro_id
                      and essa2.num_recibo = essa1.num_recibo
                      and essa2.evento_bb_id in (1154, 1156))
    and not exists (select 1
                    from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                    where essa3.sinistro_id = essa1.sinistro_id
                    and essa3.evento_id > essa1.evento_id
                    and essa3.evento_bb_id in (1190, 1100))
    order by sinistro_id,
    data_efetivacao"""

resultado_pagamento_efetuado = query_to_data_frame(query_status_pagamento_efetuado)

enviar_analise_finalizada(resultado_pagamento_efetuado.toJSON().map(lambda x: json.loads(x)).collect())
enviar_pagamento_efetuado(resultado_pagamento_efetuado.toJSON().map(lambda x: json.loads(x)).collect())

# Selecionando os sinistros com status: Aviso finalizado
logger.info("Selecionando os sinistros com status: Aviso finalizado")

query_status_aviso_finalizado = """select essa1.sinistro_id,
    essa1.categoria,
    count(essa1.sinistro_id) as qtd_eventos,
    max(essa1.data_efetivacao) as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          sin.categoria,
          essa.num_recibo,
          essa.evento_id,
          cast(pgto.dt_recebimento_cliente as date) as data_efetivacao
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on sin.sinistro_id = essa.sinistro_id
          and sin.situacao = '2'
          join spectrum_seguros_db_ab_mobile.pgto_sinistro_tb pgto
          on pgto.sinistro_id = essa.sinistro_id
          and pgto.num_recibo = essa.num_recibo
          and pgto.situacao_op = 'a'
          and pgto.item_val_estimativa = 1
          and pgto.dt_recebimento_cliente is not null
          join spectrum_seguros_db_ab_mobile.ps_acerto_pagamento_tb psa
          on psa.acerto_id = pgto.acerto_id
          and psa.voucher_id is not null
          where essa.evento_bb_id = 1152) essa1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                      where essa2.sinistro_id = essa1.sinistro_id
                      and essa2.num_recibo = essa1.num_recibo
                      and essa2.evento_bb_id in (1154, 1156))
    and not exists (select 1
                    from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                    where essa3.sinistro_id = essa1.sinistro_id
                    and essa3.evento_id > essa1.evento_id
                    and essa3.evento_bb_id in (1190, 1100))
    group by essa1.sinistro_id,
    essa1.categoria
    union
    select essa1.sinistro_id,
    essa1.categoria,
    essa1.qtd_eventos,
    essa1.dt_evento as data_evento,
    cast(date_part(day, data_evento) as integer) as dia_evento,
    cast(date_part(month, data_evento) as integer) as mes_evento,
    case mes_evento
    when 1 then 'JANEIRO'
    when 2 then 'FEVEREIRO'
    when 3 then 'MARÇO'
    when 4 then 'ABRIL'
    when 5 then 'MAIO'
    when 6 then 'JUNHO'
    when 7 then 'JULHO'
    when 8 then 'AGOSTO'
    when 9 then 'SETEMBRO'
    when 10 then 'OUTUBRO'
    when 11 then 'NOVEMBRO'
    when 12 then 'DEZEMBRO'
    end as mes_ext_evento,
    cast(date_part(year, data_evento) as integer) as ano_evento
    from (select essa.sinistro_id,
          sin.categoria,
          1 as qtd_eventos,
          essa.evento_id,
          cast(essa.dt_evento as date) as dt_evento
          from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa
          join stage_sinistro_vigencia_ab sin
          on sin.sinistro_id = essa.sinistro_id
          and sin.situacao = '2'
          where essa.evento_id = (select max(essa2.evento_id)
                                  from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa2
                                  where essa2.sinistro_id = essa.sinistro_id
                                  and essa2.evento_bb_id = 1140)) essa1
    where not exists (select 1
                      from spectrum_seguros_db_ab_mobile.evento_segbr_sinistro_atual_tb essa3
                      where essa3.sinistro_id = essa1.sinistro_id
                      and essa3.evento_id > essa1.evento_id
                      and essa3.evento_bb_id in (1190, 1100))"""

resultado_aviso_finalizado = query_to_data_frame(query_status_aviso_finalizado)

enviar_aviso_finalizado(resultado_aviso_finalizado.toJSON().map(lambda x: json.loads(x)).collect())

job.commit()
