import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model._
import redis.clients.jedis.{Jedis, Transaction}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization.write

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong


import scala.com.brasilseg.entity.entityMain

object GlueApp {

  val BLOCK_PARTITION = 10000

  val logger = new com.amazonaws.services.glue.log.GlueLogger

  var args: Map[String, String] = null

  var redisClient: Jedis = null

  var redisOperations = new AtomicLong()
  val redisTransactions = new AtomicLong()

  def initializeSparkContext: SparkContext = {
    if (args("environment") == "local") {
      new SparkContext(
        new SparkConf()
          .setAppName("local-dev")
          .setMaster("local")
          .set("spark.hadoop.mapred.output.compress", "true")
          .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
          .set("spark.hadoop.mapred.output.compression.type", "BLOCK")
      )
    } else {
      new SparkContext()
    }
  }

  private def getAWSSMParameter(parameterKey: String): String = {
    val simpleSystemsManagementClient = AWSSimpleSystemsManagementClientBuilder.standard()
      .withRegion("sa-east-1").build()
    val parameterRequest = new GetParameterRequest
    parameterRequest.withName(parameterKey).setWithDecryption(true)
    val parameterResult = simpleSystemsManagementClient.getParameter(parameterRequest)
    parameterResult.getParameter.getValue
  }

  def obterCredenciais(redshift_credentials: String): String = {

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

  def setRedis(transaction: Transaction, key: String, value: String) = {
    transaction.set(key, value)

    logger.info("Redis - #%d-%d: set %s '%s' = QUEUED".format(redisTransactions.get(), redisOperations.incrementAndGet(), key, value))
  }

  def redisTransaction(f: Function1[Transaction, Unit]): Unit = {
    val transactionId = redisTransactions.incrementAndGet()

    logger.info("Redis - Begin transaction #%d".format(transactionId))

    redisOperations = new AtomicLong()

    logger.info("Redis - multi");
    val pipeline = redisClient.multi()

    f.apply(pipeline)

    logger.info("Redis - exec");
    val result = pipeline.exec()

    var errorFound = false

    var index: Int = 0
    while (index < result.size()) {
      val value = result.get(index).toString
      val operation = index + 1
      if (value.equals("OK") || value.equals("0") || value.equals("1")) {
        logger.info("Redis - #%d-%d: +OK".format(transactionId, operation))
      } else {
        logger.error("Redis - #%d-%d: -%s".format(transactionId, operation, value))
        errorFound = true
      }
      index += 1
    }

    if (errorFound) {
      logger.error("Redis - End transaction with error #%d".format(transactionId))
      throw new RuntimeException("Ocorreu um erro ao executar a transação #%d com o Redis, verifique os logs para maiores detalhes".format(transactionId))
    } else {
      logger.info("Redis - End transaction success #%d".format(transactionId))
    }

  }

  def main(sysArgs: Array[String]) {

    val sysArgsString: String = sysArgs.mkString("Array(", ", ", ")")
    // incremental antigo
    if(sysArgsString.contains("--redshift_key_path")) {
      args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "environment", "type", "redshift_credentials", "redshift_key_path", "redis_db", "redis_port", "redis_host", "redis_ssl", "redis_auth", "redshift_schema", "schema", "filter", "queries").toArray)
    } else {
      args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "environment", "type", "redshift_credentials", "redis_db", "redis_port", "redis_host", "redis_ssl", "redis_auth", "redshift_schema", "schema", "filter", "queries", "database").toArray)
    }



    val sc: SparkContext = initializeSparkContext
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val jsonSchema = scala.io.Source.fromFile(args("schema")).mkString

    val estrutura = JSON.parseFull(jsonSchema).getOrElse(Map()).asInstanceOf[Map[String,Map[String,Map[String,List[String]]]]]

    val jsonQueries = scala.io.Source.fromFile(args("queries")).mkString

    val mapQueries = JSON.parseFull(jsonQueries).getOrElse(Map()).asInstanceOf[Map[String,_]]

    var configRedshift = JSON.parseFull(obterCredenciais(args("redshift_credentials")))

    import spark.implicits._
    implicit val formats = DefaultFormats

    val chaveForteSegbr = "plano_assistencia_id"
    val chaveForteUltron = "id_oferta_plano"
    var chaveForteFinal = chaveForteSegbr;

    var nomeChave = "plano_assistencia_id"
    var nomeChavePlanoTop = "plano_assistencia_top-residencial"

    if (args("queries").equals("AssistenciaABQuerys.json")){
      nomeChave = "plano_assistencia_vida_id"
      nomeChavePlanoTop = "plano_assistencia_top-vida"
    }

    if (args("queries").equals("AssistenciaUltronQuerys.json")){
     chaveForteFinal = chaveForteUltron;
      nomeChavePlanoTop = "plano_assistencia_top-ultron"
    }

    val typeAction = args("type")

    var condition = ""

    if (typeAction.equals("increment")){
      // Ultron é diferente a forma incremental
      if (args("queries").equals("AssistenciaUltronQuerys.json")){
        val database = args("database")
        val tableTempIds = "temp_pk_tabela_%s.chaves_redis_assistencia".format(database)
        val redshiftSchemaTemp = tableTempIds.split("\\.")(0)
        dbRedshift(tableTempIds, redshiftSchemaTemp, List(chaveForteFinal), List(), glueContext, configRedshift)
        condition = " and pl.%s IN (select %s from %s group by %s)".format(chaveForteFinal, chaveForteFinal, tableTempIds, chaveForteFinal)
      } else {
        val diretorio_parquets = args("redshift_key_path")
        val dfAssistenciaId = glueContext.read.option("compression", "gzip").parquet(diretorio_parquets)

        if (dfAssistenciaId.count() == 0) {
          logger.warn("Não existem chaves a serem atualizadas")
          return
        }
        val primeiro = dfAssistenciaId.select(nomeChave).collect().map(row=>row.get(0))
        condition = " and pl.%s IN (%s)".format(nomeChave, primeiro.mkString(", "))
      }
    }

    tempTables(args("redshift_schema"), estrutura, glueContext, configRedshift)

    val queryes = mapQueries.keys.toList.sorted

    queryes.foreach{
      query => glueContext.sql(mapQueries.get(query).get.toString).registerTempTable(query)
    }

    var queryAssistencia = mapQueries.get(queryes.last).get.toString

    queryAssistencia = queryAssistencia.concat(condition)

    val sslRedis = args("redis_ssl").toBoolean

    redisClient = new Jedis(getAWSSMParameter(args("redis_host")), getAWSSMParameter(args("redis_port")).toInt, sslRedis)
    try {
      if (sslRedis) {
        redisClient.auth(getAWSSMParameter(args("redis_auth")))
      }
      redisClient.select(args("redis_db").toInt)

      val dfRedis = glueContext.sql(queryAssistencia)
      val listaRedis = dfRedis.as[entityMain.Main].collect().grouped(BLOCK_PARTITION).toList
      listaRedis.foreach {
        particao: Array[entityMain.Main] => {
          redisTransaction { transaction =>
            particao.foreach {
              pl: entityMain.Main => {
                setRedis(transaction, "%s-%s".format(nomeChave, pl.planoId), write(pl))
                if (pl.planoReferencia) setRedis(transaction, "%s".format(nomeChavePlanoTop), write(pl))
              }
            }
          }
        }
      }
    } finally {
      redisClient.quit()
    }
  }


  def tempTables(redshiftSchema: String, result: Map[String,Map[String,Map[String,List[String]]]], glue: GlueContext, configRedshift: Option[Any]): Unit = {
    result.foreach {
      schema: (String, Map[String,Map[String,List[String]]]) => {
        print(schema._1+" tem as tabelas\n")
        val e: Array[String] = schema._1.split("\\.")
        val redshiftSchemaFinal: String = if (e.length > 1) e(0) else redshiftSchema
        schema._2.foreach{
          tableName: (String,Map[String,List[String]]) => {

            dbRedshift(tableName._1, redshiftSchemaFinal, tableName._2("pk"), tableName._2("estrutura"), glue, configRedshift)
          }
        }
      }
    }
  }

  def dbRedshift(table: String, schema: String, pks: List[String], parametros: List[String], glue: GlueContext, configRedshift: Option[Any]): Unit = {

    var valor = "*"
    if (!parametros.isEmpty) valor = parametros.distinct.mkString(",")

    val redshiftDb = configRedshift match {case Some(m: Map[String, Any]) => m("database") match { case d: String => d }}
    val redshiftUser = configRedshift match {case Some(m: Map[String, Any]) => m("usuario") match { case d: String => d }}
    val redshiftPwd = configRedshift match {case Some(m: Map[String, Any]) => m("senha") match { case d: String => d }}
    val redshiftEndPoint = configRedshift match {case Some(m: Map[String, Any]) => m("url") match { case d: String => d }}
    val redshiftPort = configRedshift match {case Some(m: Map[String, Any]) => m("porta") match { case d: String => d }}
    val redshiftTempDir = configRedshift match {case Some(m: Map[String, Any]) => m("tempdir") match { case d: String => d }}
    val awsIamRole = configRedshift match {case Some(m: Map[String, Any]) => m("aws_iam_role") match { case d: String => d }}

    var query = "select %s, row_number() over (partition by %s order by data_exportacao desc) as linha from %s.%s".format(valor, pks.mkString(","), schema, table)

    logger.info("Redshift - Executando a query: %s".format(query))

    Class.forName("com.amazon.redshift.jdbc.Driver")

    glue
      .read.format("jdbc")
      .option("url", "jdbc:redshift://%s:%s/%s"
        .format(redshiftEndPoint, redshiftPort, redshiftDb))
      .option("tempdir", redshiftTempDir)
      .option("aws_iam_role", awsIamRole)
      .option("dbtable", "(%s) foo".format(query))
      .option("user", redshiftUser)
      .option("password", redshiftPwd)
      .load.registerTempTable(table)
  }
}
