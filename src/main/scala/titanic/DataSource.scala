package titanic

import org.apache.log4j.Level
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataSource extends App {

  org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
  org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF)


  def rawTrainData(sparkSession: SparkSession): DataFrame = {

    // load train data from local
    sparkSession.read.option("header", "true").csv("/home/smileci/sample_bdd/titanic/titanic/train.csv")

  }

  def rawTestData(sparkSession: SparkSession): DataFrame = {

    // load train data from local
    // populate label col - not included in raw test data
    sparkSession.read.option("header", "true").csv("/home/smileci/sample_bdd/titanic/titanic/test.csv")
      .withColumn("Survived", lit("0"))

  }

}
