package titanic

import org.apache.spark.sql.SparkSession

// Creation de session spark


object SparkSessionCreator {

  def sparkSessionCreate(): SparkSession = {

    SparkSession
      .builder()
     // .master("local[*]")
      .appName("scala-spark-titanic-project")
      .getOrCreate()

  }

}