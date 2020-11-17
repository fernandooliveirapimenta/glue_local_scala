package assistenciaprdab

//Importações gerais do glue
import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.{SparkConf, SparkException}

// Importações gerais do spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Importações gerais do json
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

//Importações redis non-blocking
import redis._

//Importações akka
import akka.util.ByteString

// Importações scalla
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON

// Importações hadoop / URI
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

// Importações AWS Secret Manager
import java.nio.ByteBuffer
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model._
import scala.collection.JavaConversions._

object GlueApp {

  case class Resposta (
                        respostaId: Int,
                        descricaoResposta: Option[String],
                        proximaPerguntaId: Option[Int],
                        ordemResposta: Option[Int],
                        razaoResposta: Option[String],
                        tipoResposta: Option[String],
                        campoReferencia: String,
                        dadosFonteExterna: Option[Boolean]
                      )

  case class Pergunta (
                        perguntaId: Int,
                        descricaoPergunta: Option[String],
                        perguntaPreparacao: Option[Boolean],
                        multiplasRespostas: Option[Boolean],
                        razaoPergunta: Option[String],
                        tipoPergunta: Option[String],
                        respostas: Seq[Resposta]
                      )

  case class Servico (
                       servicoId: Int,
                       servicoCodigoMapfre: Option[String],
                       nomeServico: Option[String],
                       descricaoServico: Option[String],
                       franquiaValorMonetario: Option[Double],
                       franquiaQuantidade: Option[Int],
                       limiteAcionamentos: Option[Int],
                       tipoFranquiaServico: Option[String],
                       prioritario: Boolean,
                       ordem: Option[Int],
                       diasAcionamento: Option[Int],
                       itensNaoInclusos: Seq[String],
                       questionario: Seq[Pergunta]
                     )

  case class Categoria (
                         nome: Option[String],
                         servicos: Seq[Servico]
                       )

  case class PlanoAssistencia (
                                planoId: Int,
                                nome: Option[String],
                                numeroContrato: Option[Long],
                                planoReferencia: Boolean,
                                categorias: Seq[Categoria]
                              )

  val logger = new com.amazonaws.services.glue.log.GlueLogger

  var args: Predef.Map[String, String] = null

  def obterCredenciais(redshift_credentials: String): String = {

    logger.info("redshift_credentials: "+ redshift_credentials)

    val secretName: String = redshift_credentials
    val endpoint: String = "secretsmanager.us-east-1.amazonaws.com"
    val region: String = "us-east-1"
    val config: AwsClientBuilder.EndpointConfiguration =
      new AwsClientBuilder.EndpointConfiguration(endpoint, region)
    val clientBuilder: AWSSecretsManagerClientBuilder =
      AWSSecretsManagerClientBuilder.standard()
    clientBuilder.setEndpointConfiguration(config)
    val client: AWSSecretsManager = clientBuilder.build()
    var secret: String = null
    var binarySecretData: ByteBuffer = null
    val getSecretValueRequest: GetSecretValueRequest =
      new GetSecretValueRequest()
        .withSecretId(secretName)
        .withVersionStage("AWSCURRENT")
    var getSecretValueResult: GetSecretValueResult = null

    try getSecretValueResult = client.getSecretValue(getSecretValueRequest)
    catch {
      case e: ResourceNotFoundException =>
        logger.error("The requested secret " + secretName + " was not found")

      case e: InvalidRequestException =>
        logger.error("The request was invalid due to: " + e.getMessage)

      case e: InvalidParameterException =>
        logger.error("The request had invalid params: " + e.getMessage)
    }

    if (getSecretValueResult.getSecretString != null) {
      secret = getSecretValueResult.getSecretString
      return secret
    } else {
      binarySecretData = getSecretValueResult.getSecretBinary
      return binarySecretData.toString
    }

  }

  def hasStrongfiles(sc: SparkContext, bucketPath: String, strongKeyPath: String): Boolean = {
    try {
      val files = FileSystem.get(new URI(bucketPath), sc.hadoopConfiguration).listFiles(new Path(strongKeyPath), false)
      files.hasNext
    } catch {
      case e: java.io.FileNotFoundException => false
    }
  }

  def main(sysArgs: Array[String]) {

    val sc: SparkContext =  new SparkContext(
      new SparkConf()
        .setAppName("local-dev")
        .setMaster("local")
        .set("spark.hadoop.mapred.output.compress", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        .set("spark.hadoop.mapred.output.compression.type", "BLOCK"))

    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession

    import spark.implicits._
    implicit val formats = DefaultFormats

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "type", "redshift_credentials", "bucket_path", "redshift_key_path", "redis_db", "redis_port", "redis_host", "redshift_schema", "filter").toArray)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    implicit val redisDispatcher = RedisDispatcher("akka.actor.default-dispatcher")
    implicit val akkaSystem = akka.actor.ActorSystem()

    var condition = ""

    if (args("type") == "increment") {
      val bucketPath = args("bucket_path").substring(0, args("bucket_path").indexOf("/ab_extracao"))
      val redisKeyFilePath = args("redshift_key_path")
      val strongKeyPath = redisKeyFilePath.replace(bucketPath, "")

      if (hasStrongfiles(sc, bucketPath, strongKeyPath)) {
        val dfAssistenciaId = glueContext.read.option("compression", "gzip").parquet(redisKeyFilePath)
        val primeiro = dfAssistenciaId.collect().map(row => row.get(0))

        if (primeiro.isEmpty) {
          logger.warn("Não existem registros de chaves fortes à serem atualizados.")
          return
        } else {
          condition = " where pl.plano_assistencia_id IN (%s)".format(primeiro.mkString(", "))
        }
      } else {
        logger.warn("Não existem arquivos de chaves fortes à serem carregados.")
        return
      }
    }
    
    val redis =  RedisClient(args("redis_host"), args("redis_port").toInt)
    redis.select(args("redis_db").toInt)

    val configRedshift = JSON.parseFull(obterCredenciais(args("redshift_credentials")))

    val planoQuery = """ select pl.plano_assistencia_id, pl.nome_plano, pl.num_contrato, pl.plano_referencia, pl.dt_inicio_vigencia
        from %s."plano_assistencia_tb" pl
    group by pl.plano_assistencia_id, pl.nome_plano, pl.num_contrato, pl.plano_referencia, pl.dt_inicio_vigencia """.format(args("redshift_schema"), args("filter"))

    dbRedshift(planoQuery, "plano_assistencia_tb", glueContext, configRedshift)

    val categoriaQuery = """ select cat.categoria_servico_id, cat.nome 
        from %s."assistencia_categoria_servico_tb" cat
    group by cat.categoria_servico_id, cat.nome  """.format(args("redshift_schema"))

    dbRedshift(categoriaQuery, "assistencia_categoria_servico_tb", glueContext, configRedshift)

    val servLigacaoQuery = """ select ass.categoria_servico_id, ass.dt_inicio_vigencia, ass.plano_assistencia_id, ass.servico_id, ass.limite_monetario, ass.limite_quantidade, ass.quantidade_servico, ass.tipo_limite_franquia, ass.prioritario, ass.ordem, ass.dias_acionamento 
        from %s."assistencia_servico_tb" ass where usuario like '%s' 
    group by ass.categoria_servico_id, ass.dt_inicio_vigencia, ass.plano_assistencia_id, ass.servico_id, ass.limite_monetario, ass.limite_quantidade, ass.quantidade_servico, ass.tipo_limite_franquia, ass.prioritario, ass.ordem, ass.dias_acionamento """.format(args("redshift_schema"), args("filter"))

    dbRedshift(servLigacaoQuery, "assistencia_servico_tb", glueContext, configRedshift)

    val itensQuery = """ select itn.item_id, itn.servico_id, itn.plano_assistencia_id, itn.descricao, dt_inicio_vigencia
        from %s."itens_nao_inclusos_tb" as itn where itn.usuario like '%s' 
    group by itn.item_id, itn.servico_id, itn.plano_assistencia_id, itn.descricao, dt_inicio_vigencia """.format(args("redshift_schema"), args("filter"))

    dbRedshift(itensQuery, "itens_nao_inclusos_tb", glueContext, configRedshift)

    val servQuery = """ select serv.servico_id, serv.codigo_ama, serv.txt_servico, serv.descricao 
        from %s."servico_tb" serv where serv.usuario like '%s' 
    group by serv.servico_id, serv.codigo_ama, serv.txt_servico, serv.descricao """.format(args("redshift_schema"), args("filter"))

    dbRedshift(servQuery, "servico_tb", glueContext, configRedshift)

    val questQuery = """ select qst.questionario_id, qst.servico_id 
        from %s."assistencia_questionario_tb" qst where usuario like '%s' 
    group by  qst.questionario_id, qst.servico_id  """.format(args("redshift_schema"), args("filter"))

    dbRedshift(questQuery, "assistencia_questionario_tb", glueContext, configRedshift)

    val perguntaQuery = """ select req.tp_pergunta_id, req.questionario_id, req.pergunta_id, req.pergunta, req.preparacao, req.multiplas_respostas, req.razao 
        from %s."assistencia_questionario_pergunta_tb" as req where req.usuario like '%s'
    group by req.tp_pergunta_id, req.questionario_id, req.pergunta_id, req.pergunta, req.preparacao, req.multiplas_respostas, req.razao """.format(args("redshift_schema"), args("filter"))

    dbRedshift(perguntaQuery, "assistencia_questionario_pergunta_tb", glueContext, configRedshift)

    val respostaQuery =""" select res.tp_resposta_id, res.pergunta_id, res.resposta_id, res.resposta, res.ordem, res.razao, res.proxima_pergunta, res.campo_referencia, res.dados_fonte_externa
        from %s."assistencia_questionario_resposta_tb" as res where res.usuario like '%s' 
    group by res.tp_resposta_id, res.pergunta_id, res.resposta_id, res.resposta, res.ordem, res.razao, res.proxima_pergunta, res.campo_referencia, res.dados_fonte_externa  """.format(args("redshift_schema"), args("filter"))

    dbRedshift(respostaQuery, "assistencia_questionario_resposta_tb", glueContext, configRedshift)

    val tpPerguntaQuery = """ select req.tp_pergunta_id, req.nome
        from %s."assistencia_questionario_tp_pergunta_tb" as req
    group by req.tp_pergunta_id, req.nome """.format(args("redshift_schema"))

    dbRedshift(tpPerguntaQuery, "assistencia_questionario_tp_pergunta_tb", glueContext, configRedshift)

    val tpRespostaQuery =""" select res.tp_resposta_id, res.nome
        from %s."assistencia_questionario_tp_resposta_tb" as res
    group by res.tp_resposta_id, res.nome """.format(args("redshift_schema"))

    dbRedshift(tpRespostaQuery, "assistencia_questionario_tp_resposta_tb", glueContext, configRedshift)

    val rcollectQuery = """       
      select assistencia_questionario_resposta_tb.pergunta_id,
          collect_list(
              named_struct(
                  "respostaId", resposta_id
                  ,"descricaoResposta", nvl(resposta, '')
                  ,"proximaPerguntaId", nvl(proxima_pergunta, 0)
                  ,"ordemResposta", nvl(ordem, 0)
                  ,"razaoResposta", nvl(razao, '')
                  ,"tipoResposta", nvl(assistencia_questionario_tp_resposta_tb.nome, '')
                  ,"campoReferencia", nvl(campo_referencia, '')
                  ,"dadosFonteExterna",
                        case when dados_fonte_externa = 'S' then true when dados_fonte_externa = 'N' then false else false
                      end
              )
          ) AssistenciaQuestionarioRespostas
      from assistencia_questionario_resposta_tb
      left join assistencia_questionario_tp_resposta_tb
        on assistencia_questionario_resposta_tb.tp_resposta_id = assistencia_questionario_tp_resposta_tb.tp_resposta_id
      group by assistencia_questionario_resposta_tb.pergunta_id
      """

    glueContext.sql(rcollectQuery).createOrReplaceTempView("assistencia_questionario_resposta_mob")

    val pcollectQuery = """ 
        select assistencia_questionario_pergunta_tb.questionario_id,
            collect_list(
                named_struct(
                    "perguntaId", assistencia_questionario_pergunta_tb.pergunta_id
                    ,"descricaoPergunta", nvl(pergunta, '')
                    ,"perguntaPreparacao",
                        case when preparacao = 'S' then true when preparacao = 'N' then false else false
                      end
                    ,"multiplasRespostas",
                        case when multiplas_respostas = 'S' then true when multiplas_respostas = 'N' then false else false
                      end
                    ,"razaoPergunta", nvl(razao, '')
                    ,"tipoPergunta", nvl(assistencia_questionario_tp_pergunta_tb.nome, '')
                    ,"respostas", AssistenciaQuestionarioRespostas
                )
            ) AssistenciaQuestionarioPerguntas
        from assistencia_questionario_pergunta_tb
        left join assistencia_questionario_tp_pergunta_tb
          on assistencia_questionario_pergunta_tb.tp_pergunta_id = assistencia_questionario_tp_pergunta_tb.tp_pergunta_id
        left join assistencia_questionario_resposta_mob
          on assistencia_questionario_pergunta_tb.pergunta_id = assistencia_questionario_resposta_mob.pergunta_id
        group by assistencia_questionario_pergunta_tb.questionario_id
    """
    glueContext.sql(pcollectQuery).createOrReplaceTempView("assistencia_questionario_pergunta_mob")

    val qcollectQuery = """ 
        select assistencia_questionario_tb.servico_id,
               assistencia_questionario_pergunta_mob.AssistenciaQuestionarioPerguntas as questionarios
        from assistencia_questionario_tb
        left join assistencia_questionario_pergunta_mob
          on assistencia_questionario_tb.questionario_id = assistencia_questionario_pergunta_mob.questionario_id
    """
    glueContext.sql(qcollectQuery).createOrReplaceTempView("assistencia_questionario_mob")

    val sCollectQuery = """
        select servico_tb.servico_id as servicoId,
                named_struct(
                 "servicoCodigoMapfre", nvl(servico_tb.codigo_ama, '')
                 ,"nomeServico", nvl(servico_tb.txt_servico, '')
                 ,"descricaoServico", nvl(servico_tb.descricao, '')
                 ,"questionario", assistencia_questionario_mob.questionarios
               ) Servicos
            from servico_tb
            left join assistencia_questionario_mob
                on servico_tb.servico_id = assistencia_questionario_mob.servico_id
        """
    glueContext.sql(sCollectQuery).createOrReplaceTempView("servico_mob")

    val icollectQuery = """       
        select itens_nao_inclusos_tb.servico_id,
        		itens_nao_inclusos_tb.plano_assistencia_id,
        		itens_nao_inclusos_tb.dt_inicio_vigencia,
            collect_list(
                named_struct(
                    "descricao", descricao
                )
            ) ItensNaoInclusos
         from itens_nao_inclusos_tb
        group by itens_nao_inclusos_tb.servico_id,
        		itens_nao_inclusos_tb.plano_assistencia_id,
            itens_nao_inclusos_tb.dt_inicio_vigencia
     """
    glueContext.sql(icollectQuery).createOrReplaceTempView("itens_nao_inclusos_mob")

    val servAsssistQuery = """
         select
            assistencia_servico_tb.plano_assistencia_id as planoId,
            assistencia_servico_tb.dt_inicio_vigencia,
            assistencia_servico_tb.categoria_servico_id as categoriaservicoid,
            collect_list(
                named_struct(
                    "servicoId",  assistencia_servico_tb.servico_id
                    ,"servicoCodigoMapfre", servico_mob.Servicos.servicoCodigoMapfre
                    ,"nomeServico", servico_mob.Servicos.nomeServico
                    ,"descricaoServico", servico_mob.Servicos.descricaoServico
                    ,"franquiaValorMonetario", nvl(limite_monetario, 0)
                    ,"franquiaQuantidade", nvl(limite_quantidade, 0)
                    ,"limiteAcionamentos", nvl(quantidade_servico, 0)
                    ,"tipoFranquiaServico", nvl(tipo_limite_franquia, 'S')
                    ,"prioritario",
                        case when prioritario = 'S' then true when prioritario = 'N' then false else false
                      end
                    ,"ordem", nvl(ordem, 100)
                    ,"diasAcionamento", nvl(dias_acionamento, 0)
                    ,"itensNaoInclusos", itens_nao_inclusos_mob.ItensNaoInclusos.descricao
                    ,"questionario", servico_mob.Servicos.questionario
                )
            ) AssistenciaServicos
         from assistencia_servico_tb
         left join servico_mob
            on  assistencia_servico_tb.servico_id =  servico_mob.servicoId
         left join itens_nao_inclusos_mob
            on  assistencia_servico_tb.servico_id =  itens_nao_inclusos_mob.servico_id
            and assistencia_servico_tb.plano_assistencia_id =  itens_nao_inclusos_mob.plano_assistencia_id
            and assistencia_servico_tb.dt_inicio_vigencia = itens_nao_inclusos_mob.dt_inicio_vigencia
         group by
            assistencia_servico_tb.plano_assistencia_id,
            assistencia_servico_tb.dt_inicio_vigencia,
            assistencia_servico_tb.categoria_servico_id
        """
    glueContext.sql(servAsssistQuery).createOrReplaceTempView("assistencia_servico_mob")

    var catQuery = """
       select assistencia_servico_mob.planoId,
              assistencia_servico_mob.dt_inicio_vigencia,
            collect_list(
                named_struct(
                  "nome", assistencia_categoria_servico_tb.nome
                  ,"servicos", assistencia_servico_mob.AssistenciaServicos
                )
            ) Categorias
         from assistencia_categoria_servico_tb
         left join assistencia_servico_mob
           on assistencia_categoria_servico_tb.categoria_servico_id = assistencia_servico_mob.categoriaservicoid
        group by assistencia_servico_mob.planoId,
              assistencia_servico_mob.dt_inicio_vigencia
        """
    glueContext.sql(catQuery).createOrReplaceTempView("assistencia_categoria_servico_mob")

    var queryAssistencia = """
        select pl.plano_assistencia_id as planoId,
                pl.nome_plano as nome,
                cast(nvl(pl.num_contrato, '0') as LONG) as numeroContrato,
                        case when plano_referencia = 'S' then true when plano_referencia = 'N' then false else false
                      end planoReferencia,
                cmob.Categorias as categorias
            from plano_assistencia_tb as pl
            inner join assistencia_categoria_servico_mob as cmob
                on cmob.planoId = pl.plano_assistencia_id
                and cmob.dt_inicio_vigencia = pl.dt_inicio_vigencia
        """

    val dfRedis = glueContext.sql(queryAssistencia.concat(condition))

//    logger.warn(" shooooooooooooooooow ************************************")
//    dfRedis.count()

    val listaRedis = dfRedis.as[PlanoAssistencia].collect().grouped(100000).toList
    listaRedis.foreach{
      particao: Array[PlanoAssistencia] => {
        var pipeline = redis.transaction()
        particao.foreach{
          pl: PlanoAssistencia => {
            pipeline.set("plano_assistencia_vida_id-%s".format(pl.planoId), ByteString(write(pl)))
            if (pl.planoReferencia) pipeline.set("plano_assistencia_top-vida", ByteString(write(pl)))
          }
        }
        var futuraResposta = pipeline.exec()
        Await.result(futuraResposta, 10 minutes)

      }
    }
    logger.warn(" passou escrevendo redis **************8 ")
    redis.quit()
//    akkaSystem.shutdown()
  }

  def dbRedshift(nomeQuery: String, tabName: String, glue: GlueContext, configRedshift: Option[Any]): Unit = {

//    val redshiftDb = configRedshift match {case Some(m: Map[String, Any]) => m("database") match { case d: String => d }}
//    val redshiftUser = configRedshift match {case Some(m: Map[String, Any]) => m("usuario") match { case d: String => d }}
//    val redshiftPwd = configRedshift match {case Some(m: Map[String, Any]) => m("senha") match { case d: String => d }}
//    val redshiftEndPoint = configRedshift match {case Some(m: Map[String, Any]) => m("url") match { case d: String => d }}
//    val redshiftPort = configRedshift match {case Some(m: Map[String, Any]) => m("porta") match { case d: String => d }}
//    val redshiftTempDir = configRedshift match {case Some(m: Map[String, Any]) => m("tempdir") match { case d: String => d }}
//    val awsIamRole = configRedshift match {case Some(m: Map[String, Any]) => m("aws_iam_role") match { case d: String => d }}

//    connect ("jdbc:redshift://cluster-dw.catcuc4korzp.us-east-1.redshift.amazonaws.com:5439/dw_db", "admin", "Brasilseg#&!6453!");

    val redshiftDb = "dw_db"
    val redshiftUser = "admin"
    val redshiftPwd = "Brasilseg#&!6453!"
    val redshiftEndPoint = "cluster-dw.catcuc4korzp.us-east-1.redshift.amazonaws.com"
    val redshiftPort = "5439"
    val redshiftTempDir = "s3n://brasilseg-redshift-tempdir"
    val awsIamRole = "arn:aws:iam::909530209831:role/Brasilseg-RedshiftS3Role"

    logger.info("Redshift - Query: %s - %s".format(nomeQuery, tabName))

    Class.forName("com.amazon.redshift.jdbc.Driver")

    glue
      .read.format("jdbc")
      .option("url", "jdbc:redshift://%s:%s/%s"
        .format(redshiftEndPoint, redshiftPort, redshiftDb))
      .option("tempdir", redshiftTempDir)
      .option("aws_iam_role", awsIamRole)
//      .option("query", nomeQuery)
      .option("dbtable", "(%s) foo".format(nomeQuery))
      .option("user", redshiftUser)
      .option("password", redshiftPwd)
      .load.registerTempTable(tabName)
  }
}
