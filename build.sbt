name := "titanic"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" %sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.apache.commons" % "commons-csv" % "1.1"
)
