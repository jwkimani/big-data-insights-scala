name := "big-data-insights-scala"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  ("org.apache.spark" % "spark-core_2.11" % "2.1.0"),
  ("org.apache.spark" % "spark-sql_2.11" % "2.1.0"),
  "org.apache.spark" % "spark-hive_2.11" % "2.1.0",
  "com.databricks" % "spark-avro_2.11" % "3.2.0",
  "com.databricks" % "spark-csv_2.10" % "1.3.0",
  "org.scala-lang" % "scala-library" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "2.8.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.8.1",
  "org.apache.kafka" %% "kafka" % "0.9.0.2.3.4.51-1"

)
//use external repositories
resolvers += "HortonWorksRepo" at "http://repo.hortonworks.com/content/repositories/releases/"

parallelExecution in test := false


initialCommands := "import org.test._"

//clean operations
cleanFiles += baseDirectory { base => base / "build" }.value
cleanFiles += baseDirectory { base => base / "metastore_db" }.value

//assembly-settings