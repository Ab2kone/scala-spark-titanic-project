package titanic
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SaveMode}
// enregistrement des diferents sorties

object OutputSaver {

  // fonction de sauvegarde des pipeline filled
  def pipelineSaver(pipelineModel: PipelineModel): Unit = {

    pipelineModel
      .write
      .overwrite()
      .save("./pipelines/fitted-pipeline")

  }

  // fonction d'enregistrement de la prédictions
  def predictionsSaver(dataFrame: DataFrame): Unit = {

    dataFrame
      .select("PassengerId", "prediction")
      .withColumnRenamed("prediction", "Survived")
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .csv(path = "/home/smileci/sample_bdd/titanic/titanic/predictions_csv")

  }

}