package com.getindata

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}


class MovieRatingsProcessor(rawDataLocation: String)(implicit val spark: SparkSession) {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(): Unit = {
    logger.info("Hello BDTW2020")

  }
}
