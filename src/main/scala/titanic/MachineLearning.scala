package titanic

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.DataFrame

// préparation final des données pour le ML
// définition et exécutiondes modèles ML
// cela devrait généralement renvoyer des modèles de ML formés et / ou des données étiquetées
// REMARQUE ceci peut être fusionné avec feature engineering pour créer un seul pipeline

object MachineLearning {

  def pipelineFit(dataFrame: DataFrame): PipelineModel = {

    // défintion de l'assembleur de vecteurs des features
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array[String](
        "Male",
        "Embarked_Indexed_Vec",
        "Pclass_Indexed_Vec",
        "Title_Vec",
        "FamilySize_Vec",
        "FareGroup_Vec",
        "AgeGroup_Vec"
      )
      )
      .setOutputCol("features")

    // définition de random forest estimator
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
      .setMaxDepth(10)
      .setSeed(1L)

    // défintion de gbt estimator
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setSeed(1L)



    // indexeur des string pour un pipeline forest
    val pipeline = new Pipeline()
      .setStages(
        Array(
          featureAssembler,
          rf
        )
      )

    // fit pipeline
    val pipelineModel = pipeline.fit(dataFrame)

    // création de predictions
    val predictions = pipelineModel.transform(dataFrame)

    // selection (prediction, true label) calcul des erreurs de test
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    // retourne fitted pipeline
    pipelineModel

  }

}