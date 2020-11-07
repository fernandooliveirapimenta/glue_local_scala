package com.brasilseg.etl.scripts.bb30.datalake

import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.apache.spark.sql.functions.{col, lit, typedLit, when}

import scala.collection.JavaConverters._

object GeracaoCSV {

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
			"sqlserver_address",
			"sqlserver_database",
			"sqlserver_username",
			"sqlserver_password",
			"table_names",
			"sql_conditions",
			"csv_output_path",
			"op"
		).toArray)

		sc = initializeSparkContext
		glueContext = new GlueContext(sc)
		sparkSession = glueContext.getSparkSession

		val tables = args("table_names").split(";")
		val conditions = args("sql_conditions").split(";")

		var i = 0
		tables.foreach { table =>
			writeCSV(loadSQLServer(table.trim, conditions(i)), table.trim)
			i = i + 1
		}

	}

	def writeCSV(df: DataFrame, table: String) = {
		df.write.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("delimiter", "§")
			.option("escape", "\"")
			.option("dateFormat", "yyyy-MM-dd HH:mm:ss")
			.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
			.option("charset", "UTF-8")
			.mode(SaveMode.Overwrite)
			.save("%s/%s".format(args("csv_output_path"), table))

		//df.coalesce(1).write.format("com.databricks.spark.csv")
		//.option("header", "true")
		//.save("D:/ntendencia/projetos/CSV/teste.csv")
	}

	def loadSQLServer(table: String, sqlCondition: String): DataFrame = {
		logger.info("SQLServer - Consultanto tabela: %s".format(table))

		val df = sparkSession.read
			.format("jdbc")
			.option("url", "jdbc:sqlserver://%s;databaseName=%s".format(args("sqlserver_address"), args("sqlserver_database")))
			.option("user", args("sqlserver_username"))
			.option("password", args("sqlserver_password"))
			.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
			.option("dbtable", "(select * from dbo.%s %s) foo".format(table, sqlCondition))
			.load()
			.drop("lock")
			.withColumn("op", lit(args("op")))


		return df
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
				.set("spark.network.timeout", "200000")
				.set("spark.driver.memory", "10g")
				.set("spark.executor.memory", "10g")
		)
	}
}