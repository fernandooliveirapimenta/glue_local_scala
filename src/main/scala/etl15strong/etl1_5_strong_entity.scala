package etl15strong

import java.net.URI
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.glue._
import com.amazonaws.services.glue.util.{GlueArgParser, JsonOptions}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats

import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON

object GlueAppFernando {

  val logger = new com.amazonaws.services.glue.log.GlueLogger

  var sc: SparkContext = _
  var glueContext: GlueContext = _
  var sparkSession: SparkSession = _
  var s3Client: AmazonS3 = _
  var args: Map[String, String] = _
  var redshiftConfig: Map[String, String]  = _

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(sysArgs: Array[String]): Unit = {

    args = GlueArgParser.getResolvedOptions(sysArgs, Seq(
      "JOB_NAME",
      "environment",
      "schema_path",
      "domain",
      "target_key_path",
      "database",
      "redshift_credentials",
      "redshift_schema",
      "strong_entity"
    ).toArray)

    sc = initializeSparkContext
    glueContext = new GlueContext(sc)

    s3Client = initializeAmazonS3

    val spark: SparkSession = glueContext.getSparkSession

    redshiftConfig = getRedshiftConfig(args("redshift_credentials"))

    val database = args("database")

    val domains = args("domain").split(";")
    val strongEntityKey = args("strong_entity").split(";")

    val schemaCount = new AtomicInteger(0)
    for (schemaPath <- args("schema_path").split(";")) {

      var incrementStrongEntity: DataFrame = null

      logger.info("Processando o domínio: %s...".format(domains(schemaCount.get)))

      val jsonSchema = if (schemaPath.startsWith("file:") || schemaPath.startsWith("/")) {
        scala.io.Source.fromFile(schemaPath.replace("file://", "")).mkString
      } else {
        val schemaBucketName = schemaPath.split("//")(1).split("/")(0)
        val schemaObjectsPath = schemaPath.split("/", 4)(3)
        scala.io.Source.fromInputStream(s3Client.getObject(schemaBucketName, schemaObjectsPath).getObjectContent).mkString
      }

      logger.info("Schema: %s".format(jsonSchema))

      val mapSchema = JSON.parseFull(jsonSchema).getOrElse(Map()).asInstanceOf[Map[String, Object]]
      for ((catalog, tablesByCatalog) <- mapSchema) {
        for ((table, parameters) <- tablesByCatalog.asInstanceOf[Map[String, _]]) {

          logger.info("Processando a tabela: %s.dbo.%s...".format(catalog, table.toLowerCase))

          val tableParameters = parameters.asInstanceOf[Map[String, _]]

          val strongEntityKeysTablePath = args("target_key_path").format("%s/%s".format(catalog, table.toLowerCase))
          if (!getFilesDateOrdered(strongEntityKeysTablePath, "parquet", false).isEmpty) {

            logger.info("Lendo chaves que foram gravadas: %s...".format(strongEntityKeysTablePath))

            val df = glueContext.read.parquet(strongEntityKeysTablePath)

            var newKeys: DataFrame = df
            if (tableParameters.containsKey("tabela_selecao")) {
              writeRedshift(df, "temp_pk_tabela_%s.tabela_selecao_%s_%s".format(database, catalog, table.toLowerCase))

              var query = "select %s from %s.%s where (%s) in (select %s from temp_pk_tabela_%s.tabela_selecao_%s_%s)".format(
                tableParameters("parametro_selecao"), args("redshift_schema").format(catalog),
                tableParameters("tabela_selecao"), tableParameters("parametro_selecao"),
                tableParameters("valor_selecao"), database, catalog, table.toLowerCase)

              var selTableParameters = mapSchema(catalog).asInstanceOf[Map[String, _]](tableParameters("tabela_selecao").toString).asInstanceOf[Map[String, _]]
              while (selTableParameters.containsKey("tabela_selecao")) {
                query = "select %s from %s.%s where (%s) in (%s)".format(
                  selTableParameters("parametro_selecao"), args("redshift_schema").format(catalog),
                  selTableParameters("tabela_selecao"), selTableParameters("parametro_selecao"), query)
                selTableParameters = mapSchema(catalog).asInstanceOf[Map[String, _]](selTableParameters("tabela_selecao").toString).asInstanceOf[Map[String, _]]
              }

              query = "select %s from %s".format(strongEntityKey(schemaCount.get()), query.split(" from ", 2)(1))

              newKeys = readRedshift(query)
            }

            incrementStrongEntity = if (incrementStrongEntity == null) newKeys else incrementStrongEntity.union(newKeys).coalesce(1).dropDuplicates(strongEntityKey(schemaCount.get()))
          }
        }
      }

      val targetKeyPath = args("target_key_path").format(domains(schemaCount.get()))

      val protocol = if (targetKeyPath.startsWith("/") || targetKeyPath.startsWith("file:")) "file" else "s3"
      val targetKeyFilePath = extractPath(protocol, targetKeyPath)
      val targetKeyBucketName = targetKeyFilePath.split("/")(0)
      val targetKeyObjectsPath = targetKeyFilePath.split("/", 2)(1)

      logger.info("Excluindo chaves fortes antigas: %s".format(targetKeyPath))

      for (keyStrongEntity <- getFilesDateOrdered(targetKeyPath, "parquet", false)) {
        deleteFile(protocol, targetKeyBucketName, targetKeyObjectsPath, keyStrongEntity)
      }

      if (incrementStrongEntity == null) {
        incrementStrongEntity = spark.createDataFrame(List(), StructType(List(StructField(strongEntityKey(schemaCount.get()), StringType, true))))
      }

      if (args("environment") == "local") {
        incrementStrongEntity.show()
      }

      logger.info("Gravando chaves fortes novas: %s".format(targetKeyPath))

      val dyDF = DynamicFrame(incrementStrongEntity, glueContext)
      glueContext.getSinkWithFormat(connectionType = "s3",
        options = JsonOptions(Map("path" -> targetKeyPath, "compression" -> "gzip")),
        format = "parquet", transformationContext = "datasink").writeDynamicFrame(dyDF)

      schemaCount.incrementAndGet()

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

    val tempdir = redshiftCredentials.get("tempdir").mkString.concat("/%s".format(java.util.UUID.randomUUID.toString))
    logger.info("Os arquivos temporarios serão salvos no caminho: %s".format(tempdir))

    Map("url" -> "jdbc:redshift://%s:%s/%s".format(redshiftCredentials.get("url").mkString, redshiftCredentials.get("porta").mkString, redshiftCredentials.get("database").mkString),
      "driver" -> "com.amazon.redshift.jdbc.Driver",
      "user" -> redshiftCredentials.get("usuario").mkString,
      "password" -> redshiftCredentials.get("senha").mkString,
      "tempdir" -> tempdir)
  }

  def writeRedshift(df: DataFrame, dbTable: String): Unit = {
    logger.info("Redshift - Write: %s".format(dbTable))

    val start = System.currentTimeMillis

    df.write.format("jdbc")
      .options(redshiftConfig)
      .option("dbtable", dbTable)
      .mode(SaveMode.Overwrite)
      .save()

    logger.info("Redshift - Write time: %d ms".format(System.currentTimeMillis - start))
  }

  def readRedshift(query: String): DataFrame = {
    logger.info("Redshift - Execute query: %s".format(query))

    val start = System.currentTimeMillis

    val df = glueContext.read.format("jdbc")
      .options(redshiftConfig)
      .option("dbtable", "(%s) foo".format(query))
      .load()

    logger.info("Redshift - Execute query time: %d ms".format(System.currentTimeMillis - start))

    df
  }

  def getFilesDateOrdered(sourcePath: String, extension: String, recursive: Boolean): List[String] = {
    var result = List[String]()
    var mapFiles = List[Map[String,String]]()
    val directoryPath = new Path(sourcePath)
    val config = FileSystem.get(new URI(sourcePath), sc.hadoopConfiguration)
    if (config.exists(directoryPath)) {
      val files = config.listFiles(directoryPath, recursive)
      while (files.hasNext) {
        val file = files.next()
        val fileName = if (recursive) file.getPath.toString else file.getPath.getName
        if ((fileName.toLowerCase.endsWith(".%s".format(extension)) || fileName.toLowerCase.endsWith(".%s.gz".format(extension)) || fileName.toLowerCase.endsWith(".gz.%s".format(extension)))
            && fileName.toLowerCase.indexOf(".crc") == -1) {
          logger.info("Adicionando arquivo: %s - Data de modificacao: %s".format(fileName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(file.getModificationTime))))
          mapFiles = mapFiles :+ Map("FileName" -> fileName, "ModificationTime" -> file.getModificationTime.toString)
        }
      }
      mapFiles = mapFiles.sortWith(_.get("ModificationTime").getOrElse("0").toLong < _.get("ModificationTime").getOrElse("0").toLong)
    }
    for (file <- mapFiles) {
      result = result :+ file("FileName")
    }
    result
  }

  def extractPath(protocol: String, sourcePath: String) = {
    sourcePath.split(if (sourcePath.startsWith("/")) "/" else if (protocol.equals("file")) "///" else "//", 2)(1)
  }

  def deleteFile(protocol: String, sourceBucketName: String, sourceObjectsPath: String, filePath: String) = {
    val sourcePath = sourceObjectsPath.concat("/%s".format(filePath))

    logger.info("Deletando arquivo de %s/%s...".format(sourceBucketName, sourcePath))

    if (protocol.equals("file")) {
      java.nio.file.Files.delete(Paths.get("/%s".format(sourceBucketName), sourcePath))
    } else {
      s3Client.deleteObject(sourceBucketName, sourcePath)
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
}
