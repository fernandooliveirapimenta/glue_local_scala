package bkpPrd

//Importações gerais do glue
import GlueApp.args
import bkphml.GlueApp.args
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
                        descricaoResposta: String,
                        ordemResposta: Int,
                        proximaPerguntaId: Option[Int],
                        razaoResposta: String
                      )

  case class Pergunta (
                        perguntaId: Int,
                        descricaoPergunta: String,
                        perguntaPreparacao: Boolean,
                        multiplasRespostas: Boolean,
                        razaoPergunta: Option[String],
                        respostas: Seq[Resposta]
                      )

  case class Servico (
                       servicoId: Int,
                       servicoCodigoMapfre: String,
                       nomeServico: String,
                       descricaoServico: String,
                       franquiaValorMonetario: Double,
                       franquiaQuantidade: Int,
                       limiteAcionamentos: Option[Int],
                       tipoFranquiaServico: String,
                       prioritario: Boolean,
                       ordem: Int,
                       diasAcionamento: Int,
                       itensNaoInclusos: Seq[String],
                       questionario: Seq[Pergunta]
                     )

  case class PlanoAssistencia (
                                planoId: Int,
                                nome: String,
                                numeroContrato: Long,
                                planoreferencia: String,
                                servicos: Seq[Servico]
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

    // novos parametros environment,filter,queries,schema,redis_ssl,redis_auth,redshift_schema (em dev/hml)
    // parametros removidos - bucket_path
    args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "type", "redshift_credentials", "bucket_path","redshift_key_path", "redis_db", "redis_port", "redis_host").toArray)
//    args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "environment", "type", "redshift_credentials",
    //    "redshift_key_path", "redis_db", "redis_port", "redis_host", "redis_ssl", "redis_auth", "redshift_schema", "schema", "filter", "queries").toArray)
//    if(sysArgsString.contains("--redshift_key_path")) {
//      args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "environment", "type", "redshift_credentials",
    //      "redshift_key_path", "redis_db", "redis_port", "redis_host", "redis_ssl", "redis_auth", "redshift_schema", "schema", "filter", "queries").toArray)
//    } else {
//      args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "environment", "type", "redshift_credentials",
    //      "redis_db", "redis_port", "redis_host", "redis_ssl", "redis_auth", "redshift_schema", "schema", "filter", "queries", "database").toArray)

//    }


    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession

    import spark.implicits._
    implicit val formats = DefaultFormats

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    implicit val redisDispatcher = RedisDispatcher("akka.actor.default-dispatcher")
    implicit val akkaSystem = akka.actor.ActorSystem()

    var condition = ""

    if (args("type") == "increment") {
      val bucketPath = args("bucket_path")
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

    val respostaQuery ="""select res.pergunta_id, res.resposta_id, res.resposta, res.ordem, res.razao, res.proxima_pergunta from spectrum_assistencia_db_abs_mobile."assistencia_questionario_resposta_tb" as res where res.usuario like 'CARGA_BB_3.0' group by res.pergunta_id, res.resposta_id, res.resposta, res.ordem, res.razao, res.proxima_pergunta """

    dbRedshift(respostaQuery, "resposta_mob", glueContext, configRedshift)

    val perguntaQuery = """ select qst.questionario_id, req.pergunta_id, req.pergunta, req.preparacao, req.multiplas_respostas, req.razao from spectrum_assistencia_db_abs_mobile."assistencia_questionario_pergunta_tb" as req left join spectrum_assistencia_db_abs_mobile."assistencia_questionario_tb" qst     on qst.questionario_id = req.questionario_id where req.usuario like 'CARGA_BB_3.0' group by  qst.questionario_id, req.pergunta_id, req.pergunta, req.preparacao, req.multiplas_respostas, req.razao """

    dbRedshift(perguntaQuery, "pergunta_mob", glueContext, configRedshift)

    val questQuery = """ select qst.questionario_id, qst.servico_id from spectrum_assistencia_db_abs_mobile."assistencia_questionario_tb" qst where usuario like 'CARGA_BB_3.0' group by  qst.questionario_id, qst.servico_id  """

    dbRedshift(questQuery, "assistencia_questionario_mob", glueContext, configRedshift)

    val itensQuery = """ select itn.item_id, itn.servico_id, itn.plano_assistencia_id, itn.descricao from spectrum_assistencia_db_abs_mobile."itens_nao_inclusos_tb" as itn where itn.usuario like 'CARGA_BB_3.0' group by itn.item_id, itn.servico_id, itn.plano_assistencia_id, itn.descricao """

    dbRedshift(itensQuery, "itens_nao_inclusos_mob", glueContext, configRedshift)

    val planoQuery = """ select pl.plano_assistencia_id, pl.nome_plano, pl.num_contrato, pl.plano_referencia from spectrum_assistencia_db_abs_mobile."plano_assistencia_tb" pl where usuario like 'CARGA_BB_3.0' group by pl.plano_assistencia_id, pl.nome_plano, pl.num_contrato, pl.plano_referencia """

    dbRedshift(planoQuery, "plano_assistencia_mob", glueContext, configRedshift)

    val servQuery = """ select serv.servico_id, serv.codigo_ama, serv.txt_servico, serv.descricao from spectrum_assistencia_db_abs_mobile."servico_tb" serv where serv.usuario like 'CARGA_BB_3.0' group by serv.servico_id, serv.codigo_ama, serv.txt_servico, serv.descricao """

    dbRedshift(servQuery, "servico_mob", glueContext, configRedshift)

    val servLigacaoQuery = """ select ass.plano_assistencia_id, ass.servico_id, ass.limite_monetario, ass.limite_quantidade, ass.quantidade_servico, ass.tipo_limite_franquia, ass.prioritario, ass.ordem, ass.dias_acionamento from spectrum_assistencia_db_abs_mobile."assistencia_servico_tb" ass where usuario like 'CARGA_BB_3.0' group by ass.plano_assistencia_id, ass.servico_id, ass.limite_monetario, ass.limite_quantidade, ass.quantidade_servico, ass.tipo_limite_franquia, ass.prioritario, ass.ordem, ass.dias_acionamento """

    dbRedshift(servLigacaoQuery, "assistencia_servico_mob", glueContext, configRedshift)

    val rcollectQuery = """
        select rmob.pergunta_id,
             collect_list(
               named_struct(
                  'respostaId', rmob.resposta_id,
                  'descricaoResposta', nvl(rmob.resposta, ''),
                  'ordemResposta', nvl(rmob.ordem, 0),
                  'proximaPerguntaId', rmob.proxima_pergunta,
                  'razaoResposta', nvl(rmob.razao, '')
                  )
              ) as Respostas
              from resposta_mob rmob
              group by rmob.pergunta_id
     """

    glueContext.sql(rcollectQuery).createOrReplaceTempView("resposta_tb")

    val pcollectQuery = """
        select pmob.questionario_id,
              collect_list(
                named_struct(
                  'perguntaId', pmob.pergunta_id,
                  'descricaoPergunta', nvl(pmob.pergunta, ''),
                  'perguntaPreparacao',
                          case
                            when pmob.preparacao = 'S'
                              then true
                            when pmob.preparacao = 'N'
                              then false
                            else
                              false
                          end,
                  'multiplasRespostas',
                          case
                            when pmob.multiplas_respostas = 'S'
                              then true
                            when pmob.multiplas_respostas = 'N'
                              then false
                            else
                              false
                          end,
                  'razaoPergunta', pmob.razao,
                  'respostas', rmob.Respostas
                 )
              ) as Perguntas
                from pergunta_mob pmob
              left join resposta_tb rmob
                on pmob.pergunta_id = rmob.pergunta_id
              group by pmob.questionario_id
    """

    glueContext.sql(pcollectQuery).createOrReplaceTempView("pergunta_tb")

    val icollectQuery = """
        select itn.servico_id,
        itn.plano_assistencia_id,
            collect_list(
              named_struct(
                'descricaoItem', nvl(itn.descricao, '')
                  )
                ) as Descricao
              from itens_nao_inclusos_mob as itn
              group by itn.servico_id, itn.plano_assistencia_id
     """

    glueContext.sql(icollectQuery).createOrReplaceTempView("itens_tb")

    val servAsssistQuery = """
          select ass.plano_assistencia_id,
                  collect_list(
                    named_struct(
                      'servicoId', serv.servico_id,
                      'servicoCodigoMapfre', nvl(serv.codigo_ama, ''),
                      'nomeServico', nvl(serv.txt_servico, ''),
                      'descricaoServico', nvl(serv.descricao, ''),
                      'franquiaValorMonetario', nvl(ass.limite_monetario, 0),
                      'franquiaQuantidade', nvl(ass.limite_quantidade, 0),
                      'limiteAcionamentos', nvl(ass.quantidade_servico, 0),
                      'tipoFranquiaServico', nvl(ass.tipo_limite_franquia, 'S'),
                      'prioritario',
                      case
                        when ass.prioritario = 'S'
                          then true
                        when ass.prioritario = 'N'
                          then false
                        else
                          false
                      end,
                      'ordem', nvl(ass.ordem, 100),
                      'diasAcionamento', nvl(ass.dias_acionamento, 0),
                      'itensNaoInclusos', itn.Descricao.descricaoItem,
                      'questionario', pqrg.Perguntas
                      )
                  ) Servicos
            from servico_mob as serv
            left join assistencia_servico_mob as ass
              on  ass.servico_id = serv.servico_id
            left join itens_tb as itn
              on itn.servico_id = ass.servico_id
              and itn.plano_assistencia_id = ass.plano_assistencia_id
            left join assistencia_questionario_mob qst
              on qst.servico_id = serv.servico_id
            left join pergunta_tb pqrg
              on qst.questionario_id = pqrg.questionario_id
            group by ass.plano_assistencia_id
        """

    glueContext.sql(servAsssistQuery).createOrReplaceTempView("lista_tb")

    var queryAssistencia = "select "+
      "pl.plano_assistencia_id as planoId, "+
      "nvl(pl.nome_plano, '') as nome, "+
      "cast(nvl(pl.num_contrato, '0') as LONG) as numeroContrato, "+
      "nvl(pl.plano_referencia, '') as planoreferencia, "+
      "smob.Servicos as servicos "+
      "from plano_assistencia_mob as pl "+
      "left join lista_tb as smob "+
      "on smob.plano_assistencia_id = pl.plano_assistencia_id "

    val dfRedis = glueContext.sql(queryAssistencia.concat(condition))

    val listaRedis = dfRedis.as[PlanoAssistencia].collect().grouped(100000).toList
    listaRedis.foreach{
      particao: Array[PlanoAssistencia] => {
        var pipeline = redis.transaction()
        particao.foreach{
          pl: PlanoAssistencia => {
            pipeline.set("plano_assistencia_id-%s".format(pl.planoId), ByteString(write(pl)))
            if (pl.planoreferencia.toUpperCase == "S".toUpperCase) pipeline.set("plano_assistencia_top-residencial", ByteString(write(pl)))
          }
        }
        var futuraResposta = pipeline.exec()
        Await.result(futuraResposta, 10 minutes)

      }
    }
    redis.quit()
//    akkaSystem.shutdown()
  }

  def dbRedshift(nomeQuery: String, tabName: String, glue: GlueContext, configRedshift: Option[Any]): Unit = {

    val redshiftDb = configRedshift match {case Some(m: Map[String, Any]) => m("database") match { case d: String => d }}
    val redshiftUser = configRedshift match {case Some(m: Map[String, Any]) => m("usuario") match { case d: String => d }}
    val redshiftPwd = configRedshift match {case Some(m: Map[String, Any]) => m("senha") match { case d: String => d }}
    val redshiftEndPoint = configRedshift match {case Some(m: Map[String, Any]) => m("url") match { case d: String => d }}
    val redshiftPort = configRedshift match {case Some(m: Map[String, Any]) => m("porta") match { case d: String => d }}
    val redshiftTempDir = configRedshift match {case Some(m: Map[String, Any]) => m("tempdir") match { case d: String => d }}
    val awsIamRole = configRedshift match {case Some(m: Map[String, Any]) => m("aws_iam_role") match { case d: String => d }}

    logger.info("Redshift - Query: %s - %s".format(nomeQuery, tabName))
    
    glue
      .read.format("com.databricks.spark.redshift")
      .option("url", "jdbc:redshift://%s:%s/%s"
        .format(redshiftEndPoint, redshiftPort, redshiftDb))
      .option("tempdir", redshiftTempDir)
      .option("aws_iam_role", awsIamRole)
      .option("query", nomeQuery)
      .option("user", redshiftUser)
      .option("password", redshiftPwd)
      .load.createOrReplaceTempView(tabName)

  }

}