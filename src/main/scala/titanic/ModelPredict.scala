package titanic

import org.apache.spark.ml.PipelineModel

object ModelPredict extends App {



    // création d'un spark session
    val spark = SparkSessionCreator.sparkSessionCreate()

    // train data
    val rawTestData = DataSource.rawTestData(sparkSession = spark)

    // clean train data
    val cleanTestData = DataCleaner.cleanData(dataFrame = rawTestData)

    // feature data
    val featureTestData = FeatureEngineering.featureData(dataFrame = cleanTestData)

    // load fitted pipeline
    val fittedPipeline = PipelineModel.load("/home/smileci/sample_bdd/titanic/titanic/fitted-pipeline")

    // make predictions
    val predictions = fittedPipeline.transform(dataset = featureTestData)

    // save predictions
    // cela enregistre un csv au format requis par kaggle
    // c'est à dire. seulement le passager_id et les cols survivants
    OutputSaver.predictionsSaver(dataFrame = predictions)



}