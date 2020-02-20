package com.getindata

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.io.Directory
import scala.util.Try

object MovieRatingsAppTestSuite {
  val rootTestDir: String = getClass.getResource("/").getPath
  val HDFSTempDirName: String = "hdfs"
}

class MovieRatingsAppTestSuite extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  import MovieRatingsAppTestSuite._

  val logger: Logger = LoggerFactory.getLogger(getClass)

  override val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .set("spark.driver.host", "localhost")
    .set("hive.msck.path.validation", "ignore")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    .set("hive.exec.compress.output", "true")
    .set("hive.exec.compress.intermediate", "true")
    .set("spark.sql.catalogImplementation", "hive")
    .setAppName("Test")

  override lazy val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  before {
    Try {
      new Directory(new File(f"${rootTestDir.split("/target")(0)}/metastore_db/")).deleteRecursively()
    }
    Try {
      new Directory(new File(f"${rootTestDir.split("/target")(0)}/spark-warehouse/")).deleteRecursively()
    }

  }

  test("SparkApp can execute job succesfully") {
    implicit val sparkSession: SparkSession = spark

    val processor = new MovieRatingsProcessor(f"$rootTestDir/$HDFSTempDirName/movie_lens/raw/")
    processor.run()



  }

  def writeToJson(df: DataFrame, name: String): Unit = {
    df.coalesce(1).write.json(f"$rootTestDir/$HDFSTempDirName/movie_lens/raw/${name}_json")
  }

  def writeToFile(df: DataFrame, name: String): Unit = {
    df.coalesce(1).write.option("delimiter", ";" ).csv(f"$rootTestDir/$HDFSTempDirName/movie_lens/raw/$name")
  }

}