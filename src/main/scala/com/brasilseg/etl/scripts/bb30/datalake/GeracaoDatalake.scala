package com.brasilseg.etl.scripts.bb30.datalake

import java.text.SimpleDateFormat
import java.util.Date

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.apache.spark.sql.functions.{col, lit, typedLit, when}
import org.spark_project.jetty.util.thread.ThreadPool

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

//jdbc:sqlserver://10.0.35.156\QUALID_ABS:1500;databaseName=seguros_db"
//Ip: 10.0.35.191
//Porta: 1410
//jdbc:sqlserver://10.0.35.191\DESENV:1410;databaseName=seguros_db"
//dbo.formulario_tp_documento_aviso_sinistro_tb
object GeracaoDatalake {

  var sc: SparkContext = _
  var glueContext: GlueContext = _
  var sparkSession: SparkSession = _
  var args: Map[String, String] = _

  implicit val formats: DefaultFormats.type = DefaultFormats

  val logger: Logger = LogManager.getRootLogger

  def main(sysArgs: Array[String]): Unit = {

    logger.setLevel(Level.INFO)

    args = GlueArgParser.getResolvedOptions(sysArgs, Seq(
      "JOB_NAME",
      "url",
      "username",
      "password",
      "select_columns",
      "order_by",
      "show_header",
      "table_names",
      "sql_conditions",
      "num_partitions",
      "lower_bound",
      "upper_bound",
      "output_path",
      "column_partition",
      "format",
      "op"
    ).toArray)

    sc = initializeSparkContext
    glueContext = new GlueContext(sc)
    sparkSession = glueContext.getSparkSession

    val tables = args("table_names").split(";")
    val conditions = args("sql_conditions").split(";")

    var i = 0
    tables.foreach { table =>
      saveDatalake(table, conditions, i)
      i = i + 1
    }

  }

  def saveDatalake(table: String,  conditions: Array[String], i: Int): Unit = {
    writeDatalake(loadSQLJDBC(table.trim, if (conditions.length > i) conditions(i) else ""), table.trim)
  }

  def writeDatalake(df: DataFrame, table: String) = {
    val format = args("format")
    val filePath = "%s/%s".format(args("output_path"), table.replace("#", ""))

    logger.info("Escrevendo arquivo em: %s".format(filePath))

    if (format.equalsIgnoreCase("csv")) {
      df.write.format("com.databricks.spark.csv")
        .option("header", args("show_header"))
        .option("delimiter", "§")
        .option("escape", "\"")
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .mode(SaveMode.Append)
        .save(filePath)
    } else if (format.equalsIgnoreCase("parquet")) {
      df.write
        .mode(SaveMode.Overwrite)
        .parquet(filePath)
    }
  }

  def loadSQLJDBC(table: String, sqlCondition: String): DataFrame = {
    val query = "(select * from %s %s) as foo".format(table, sqlCondition).replace("*", args("select_columns"))

    logger.info("JDBC - Consultando query: %s".format(query))

    var prepareDF = sparkSession.read
      .format("jdbc")
      .option("url", args("url"))
      .option("user", args("username"))
      .option("password", args("password"))
      .option("dbtable", query)
      .option("fetchsize", "10000")

    if (args("url").indexOf("sqlserver") >= 0) {
      prepareDF = prepareDF.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    } else if (args("url").indexOf("redshift") >= 0) {
      prepareDF = prepareDF.option("driver", "com.amazon.redshift.jdbc.Driver")
    }

    if (table.startsWith("#")) {
      prepareDF = prepareDF.option("partitionColumn", args("column_partition"))
        .option("numPartitions", args("num_partitions"))
        .option("lowerBound", args("lower_bound"))
        .option("upperBound", args("upper_bound"))
    }

    var df = prepareDF.load().drop("lock")

    if (table.startsWith("#")) {
      df = df.drop(args("column_partition"))
    }

    if (!args("op").equals("-")) {
      df =  df.withColumn("op", lit(args("op")))
    }

    if (args("op").equals("U")) {
      df = df.withColumn("data_exportacao", lit(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)))
    }

    if (!args("select_columns").equals("*")) {
      df = df.select(args("select_columns").split(",").map(c => col(c)): _*)
    }

    if (!args("order_by").equals("-")) {
      df = df.orderBy(args("order_by").split(",").map(c => col(c)): _*)
    }

    return df.coalesce(1)
  }

  def initializeSparkContext: SparkContext = {
    new SparkContext(
      new SparkConf()
        .setAppName("local-dev")
        .setMaster("local")
        .set("spark.databricks.io.cache.enabled", "true")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.hadoop.mapred.output.compress", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        .set("spark.hadoop.mapred.output.compression.type", "BLOCK")
        .set("spark.network.timeout", "999000")
        .set("spark.driver.memory", "10g")
        .set("spark.executor.memory", "10g")
      )
  }
}
