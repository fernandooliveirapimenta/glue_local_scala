package etl15strong

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.services.secretsmanager._
import com.amazonaws.services.secretsmanager.model._

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON
import scala.concurrent._

object GlueApp {

  val BEGIN_EXPORT_DATE = "2020-09-22 00:00:00.000"

  val logger = new com.amazonaws.services.glue.log.GlueLogger

  var sc: SparkContext = null
  var glueContext: GlueContext = null

  var s3Client: AmazonS3 = _

  var args: Map[String, String] = null
  var redshiftConfig: Map[String, String]  = null

  def initializeSparkContext: SparkContext = {
    if (args("environment") == "local") {
      new SparkContext(
        new SparkConf()
          .setAppName("local-dev")
          .setMaster("local")
          .set("spark.hadoop.mapred.output.compress", "true")
          .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
          .set("spark.hadoop.mapred.output.compression.type", "BLOCK")
          .set("spark.sql.codegen.aggregate.map.twolevel.enabled", "false")
      )
    } else {
      new SparkContext()
    }
  }

  def initializeAmazonS3(): AmazonS3Client = {
    try {
      val s3Client: AmazonS3Client = if (args("environment").equals("local")) {
        new AmazonS3Client(new ProfileCredentialsProvider())
      } else {
        new AmazonS3Client()
      }
      s3Client
    } catch {
      case e: ExceptionInInitializerError => {
        logger.error("An error occurred while getting credentials and creating s3Client object")
        throw e
      }
    }
  }

  def getRedshiftConfig(secretName: String): Map[String, String] = {

    val endpoint: String = "secretsmanager.us-east-1.amazonaws.com"
    val region: String = "us-east-1"
    val config = new AwsClientBuilder.EndpointConfiguration(endpoint, region)
    val clientBuilder = AWSSecretsManagerClientBuilder.standard()
    clientBuilder.setEndpointConfiguration(config)

    val client = clientBuilder.build()
    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName).withVersionStage("AWSCURRENT")
    val getSecretValueResult = client.getSecretValue(getSecretValueRequest)

    var secret: String = null
    if (getSecretValueResult.getSecretString != null) {
      secret = getSecretValueResult.getSecretString
    } else {
      secret = getSecretValueResult.getSecretBinary.toString
    }
    val redshiftCredentials = JSON.parseFull(secret).getOrElse(Map()).asInstanceOf[Map[String,String]]

    Map("url" -> "jdbc:redshift://%s:%s/%s".format(redshiftCredentials.get("url").mkString, redshiftCredentials.get("porta").mkString, redshiftCredentials.get("database").mkString),
      "driver" -> "com.amazon.redshift.jdbc.Driver",
      "user" -> redshiftCredentials.get("usuario").mkString,
      "password" -> redshiftCredentials.get("senha").mkString,
      "aws_iam_role" -> redshiftCredentials.get("aws_iam_role").mkString)
  }

  def executeSQLRedshift(conn: Connection, sql: String) {
    logger.info("Redshift - Execute statement: %s".format(sql))

    val start = System.currentTimeMillis

    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      statement.execute()
    } catch {
      case e: Exception => {
        if (e.getMessage().contains("Table") && e.getMessage().contains("not found.")) {
          logger.warn(e.getMessage)
          return
        }
        throw e
      }
    } finally {
      if (statement != null) statement.close
    }

    logger.info("Redshift - Execute statement time: %d ms".format(System.currentTimeMillis - start))
  }

  def main(sysArgs: Array[String]) {

    args = GlueArgParser.getResolvedOptions(sysArgs,
      Seq("JOB_NAME", "environment", "redshift_credentials", "databases", "schemas", "entities", "keys").toArray)

    sc = initializeSparkContext
    glueContext = new GlueContext(sc)

    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    s3Client = initializeAmazonS3

    redshiftConfig = getRedshiftConfig(args("redshift_credentials"))

    val databases = args("databases").split(";")
    val schemas = args("schemas").split(";")
    val entities = args("entities").split(";")
    val keys = args("keys").split(";")

    Class.forName(redshiftConfig.get("driver").mkString)
    val conn = DriverManager.getConnection(redshiftConfig.get("url").mkString, redshiftConfig.get("user").mkString, redshiftConfig.get("password").mkString)
    try {
      var index = 0;
      for (schemaPath <- schemas) {
          
        val database = databases(index)

        logger.info("Database: %s".format(database))

        val entity = entities(index)

        logger.info("Entity: %s".format(entity))

        val strongKey = keys(index)

        logger.info("Strong Key: %s".format(strongKey))

        val schemaPathDatabase = schemaPath.format(database)

        val jsonSchema = if (schemaPathDatabase.startsWith("file:") || schemaPathDatabase.startsWith("/")) {
          scala.io.Source.fromFile(schemaPathDatabase.replace("file://", "")).mkString
        } else {
          val schemaBucketName = schemaPathDatabase.split("//")(1).split("/")(0)
          val schemaObjectsPath = schemaPathDatabase.split("/", 4)(3)
          scala.io.Source.fromInputStream(s3Client.getObject(schemaBucketName, schemaObjectsPath).getObjectContent).mkString
        }

        logger.info("Schema: %s".format(jsonSchema))

        val mapSchema = JSON.parseFull(jsonSchema).getOrElse(Map()).asInstanceOf[Map[String, Object]]

        val keyUpdate = "%s_%s".format(entity, database)

        val currentDate = getLastUpdate(conn, keyUpdate);
        val newDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())

        val tableTempIds = "temp_pk_tabela_%s.chaves_redis_%s".format(database, entity)

        createStrongEntity(conn, currentDate, newDate, database, tableTempIds, strongKey, mapSchema)

        setLastDate(conn, keyUpdate, newDate, !currentDate.equals(BEGIN_EXPORT_DATE))

        index = index + 1
      }
    } finally {
      conn.close()
    }
  }

  def createStrongEntity(conn: Connection, currentDate: String, newDate: String, database: String, tableTempIds: String, strongKey: String, mapSchema: Map[String, Object]): Unit = {

    //DROP TABLE IF EXISTS temp_pk_tabela_ultron.chaves_redis_assistencia; CREATE TABLE temp_pk_tabela_ultron.chaves_redis_assistencia (id_pacote_assistencia CHARACTER VARYING(256) ENCODE LZO);
    executeSQLRedshift(conn, "DROP TABLE IF EXISTS %1$s; CREATE TABLE %1$s (%2$s CHARACTER VARYING(256) ENCODE LZO);".format(tableTempIds, strongKey))

    for ((catalog, tablesByCatalog) <- mapSchema) {
      val redshiftSchema = "spectrum_%s_%s".format(catalog, database)

      for ((table, parameters) <- tablesByCatalog.asInstanceOf[Map[String, _]]) {

        logger.info("Processando a tabela: %s.%s...".format(redshiftSchema, table.toLowerCase))

        val tableParameters = parameters.asInstanceOf[Map[String, _]]

        val pk = tableParameters("pk").asInstanceOf[List[String]]

        var query = "select %s from %s.%s where data_exportacao > '%s' and data_exportacao <= '%s'"
        var canExecute = true

        if (tableParameters.contains("tabela_selecao")) {

          query = query.format(tableParameters("valor_selecao"), redshiftSchema, table, currentDate, newDate)

          var selTableParameters = tableParameters
          do {
            query = "select %s from %s.%s where (%s) in (%s)".format(
              selTableParameters("parametro_selecao"), redshiftSchema,
              selTableParameters("tabela_selecao"), selTableParameters("parametro_selecao"), query)

            selTableParameters = mapSchema(catalog).asInstanceOf[Map[String, _]](selTableParameters("tabela_selecao").toString).asInstanceOf[Map[String, _]]
          } while (selTableParameters.contains("tabela_selecao"))

          query = "select %s from %s".format(strongKey, query.split(" from ", 2)(1))

          logger.info("Lendo chaves fortes da tabela secundária %s.%s...".format(redshiftSchema, table))
        } else if (pk.contains(strongKey)) {
          query = query.format(strongKey, redshiftSchema, table, currentDate, newDate)

          logger.info("Lendo chaves fortes da tabela principal %s.%s...".format(redshiftSchema, table))
        } else {
          canExecute = false
          
          logger.warn("Tabela %s.%s sem relação com chave forte".format(redshiftSchema, table))
        }

        if (canExecute) executeSQLRedshift(conn, "INSERT INTO %s (%s) %s;".format(tableTempIds, strongKey, query))
      }
    }
  }

  def getLastUpdate(conn: Connection, keyPath: String): String = {

    executeSQLRedshift(conn, "CREATE TABLE IF NOT EXISTS info_etl2 (key_path CHARACTER VARYING(256) ENCODE LZO, data_exportacao CHARACTER VARYING(256) ENCODE LZO);")

    logger.info("Redshift - Read ETL Info - %s".format(keyPath))

    val sql = "select key_path, data_exportacao from info_etl2 where key_path = '%s'".format(keyPath)

    logger.info("Redshift - Execute query: %s".format(sql))

    val start = System.currentTimeMillis

    var result: String = BEGIN_EXPORT_DATE
    val statement = conn.prepareStatement(sql)
    try {
      val rs = statement.executeQuery()
      if (rs.next()) {
        result = rs.getString(2)
      }
    } finally {
      statement.close
    }

    logger.info("Redshift - Execute query time: %d ms".format(System.currentTimeMillis - start))

    result
  }

  def setLastDate(conn: Connection, keyPath: String, lastDate: String, exists: Boolean): Unit = {
    logger.info("Redshift - Write ETL Info - %s: %s".format(keyPath, lastDate))

    executeSQLRedshift(conn, if (exists) "UPDATE info_etl2 SET data_exportacao = '%s' WHERE key_path = '%s'".format(lastDate, keyPath)
      else "INSERT INTO info_etl2 (key_path, data_exportacao) VALUES ('%s', '%s')".format(keyPath, lastDate))
  }

}

