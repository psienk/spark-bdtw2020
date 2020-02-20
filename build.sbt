name := "DevelopingProductionReadySparkApplication"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion, //% "provided",
  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion, //% "provided",
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.2.0_0.7.4" % "test" excludeAll ExclusionRule(organization = "javax.servlet") excludeAll ExclusionRule("org.apache.hadoop"),
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.rogach" %% "scallop" % "3.1.2",
  "ch.cern.sparkmeasure" %% "spark-measure" % "0.15",
  "com.amazon.deequ" % "deequ" % "1.0.1"
)