package com.getindata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

object MovieRatingsApp {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  class CommandLineOpts(args: Array[String]) extends ScallopConf(args) {
    val rawDataLocation: ScallopOption[String] = opt[String](required = false)
    verify()
    logCommandLineOptions(this)
  }

  def main(args: Array[String]): Unit = {

    logger.info("Starting application. Now will parse command line options")
    val opts = new CommandLineOpts(args)
    logger.info("Creating Spark Session object")
    implicit val sparkSession: SparkSession = getSparkSession()
    val processor = new MovieRatingsProcessor(opts.rawDataLocation())
    processor.run()
  }


  def getSparkSession(): SparkSession = {
    val conf = new SparkConf()
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.compress.output", "true")
      .set("hive.exec.compress.intermediate", "true")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.orc.enableVectorizedReader", "true")
      .set("spark.sql.orc.enabled", "true")
      .set("spark.sql.orc.filterPushdown", "true")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark

  }

  def logCommandLineOptions(opts: CommandLineOpts): Unit = {
    logger.info(f"Provided raw-data-location: '${opts.rawDataLocation()}'")
  }

}
