package titanic

import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{split, when}



object FeatureEngineering {

  // fonction qui renvoie un bloc de données avec des fonctionnalités supplémentaires
  def featureData(dataFrame: DataFrame): DataFrame = {


    // fonction pour indexer la colonne embarked
    def embarkedIndexer(dataFrame: DataFrame): DataFrame = {

      val indexer = new StringIndexer()
        .setInputCol("Embarked")
        .setOutputCol("Embarked_Indexed")

      val indexed = indexer.fit(dataFrame).transform(dataFrame)

      indexed

    }

    // fonction pour indexer la colonne Pclass
    def pclassIndexer(dataFrame: DataFrame): DataFrame = {

      val indexer = new StringIndexer()
        .setInputCol("Pclass")
        .setOutputCol("Pclass_Indexed")

      val indexed = indexer.fit(dataFrame).transform(dataFrame)

      indexed

    }


    // fonction de création d'une tranche d'âge
    def ageBucketizer(dataFrame: DataFrame): DataFrame = {


      // Définition des fragments de tranche d'âge
      val ageSplits = Array(0.0, 5.0, 18.0, 35.0, 60.0, 150.0)


      // Normalisation des âges
      val ageBucketizer = new Bucketizer()
        .setInputCol("Age")
        .setOutputCol("AgeGroup")
        .setSplits(ageSplits)


      //Ajout de la tranche d'âge au données
      ageBucketizer.transform(dataFrame).drop("Age")

    }

    // même chose précedemment
    def fareBucketizer(dataFrame: DataFrame): DataFrame = {


      val fareSplits = Array(0.0, 10.0, 20.0, 30.0, 50.0, 100.0, 1000.0)


      val fareBucketizer = new Bucketizer()
        .setInputCol("Fare")
        .setOutputCol("FareGroup")
        .setSplits(fareSplits)


      fareBucketizer.transform(dataFrame).drop("Fare")

    }


    // Fonction de convertion de la colonne sex en binaire
    def sexBinerizer(dataFrame: DataFrame): DataFrame = {

      // Ajout des valeurs binaire à la colonne
      val outputDataFrame = dataFrame.withColumn("Male", when(dataFrame("Sex").equalTo("male"), 1).otherwise(0))

      // retour de data frame avec une valeur binaire de la colonne sex avec suppression de l'ancienne colonne
      outputDataFrame.drop("Sex")

    }

    // Fonction de creation du titre
    def titleCreator(dataFrame: DataFrame): DataFrame = {

      // extraction du champs titre de la colonnes nom
      val outputData = dataFrame
        .withColumn("Title_String", split(split(dataFrame("Name"), ",")(1), "[.]")(0))

      val indexer = new StringIndexer()
        .setInputCol("Title_String")
        .setOutputCol("Title")

      val indexed = indexer.fit(outputData).transform(outputData)

      indexed.drop("Name")

    }


    // fonction de création du champs taille pour faamille
    def familySizeCreator(dataFrame: DataFrame): DataFrame = {

      // création d'un champ indiquant le nombre de personnes dans une famille
      val inputData = dataFrame.withColumn("FamilyMembers", dataFrame("Parch") + dataFrame("SibSp"))


      // création d'un champ de taille de famille pour le champ précédent
      val outputData = inputData.withColumn("FamilySize",
        when(inputData("FamilyMembers")===0, 1)
          .when(inputData("FamilyMembers")>0 && inputData("FamilyMembers")<4, 2)
          .otherwise(3))


      // retourne le data frame avec un champ de taille de famille ajouté et supprime les colonnes d'origine utilisées
      outputData.drop("FamilyMembers").drop("Parch").drop("SibSp")

    }


    // définir une fonction pour créer des variables factices à partir des colonnes d'entrée
    // NOTE: cela sera remplacé par un encodeur à chaud
    def dummyCreator(dataFrame: DataFrame, dummyCols: Array[String]): DataFrame = {

      // création d'une data frame immuable temporaire à utiliser dans la boucle for
      var data = dataFrame

      // fonction pour créer des colonnes binaires pour chaque valeur unique dans les colonnes d'entrée
      for(col <- dummyCols) {

        // crée une liste de valeurs uniques dans la colonne d'entrée
        val uniqueValues = data.select(col).distinct().collect()

        // crée une colonne binaire pour chaque valeur dans une liste de valeurs uniques
        for(i <- uniqueValues.indices) {

          // définir le nom de la nouvelle colonne comme valeur unique
          var colName = uniqueValues(i).get(0)

          // supprimer les caractères spéciaux du nom pour la nouvelle colonne
          colName = colName.toString.replaceAll("[.]", "_")

          // ajoute une colonne binaire à la trame de données mutable
          data = data.withColumn(col+"_"+colName, when(data(col)===uniqueValues(i)(0), 1).otherwise(0))
        }

        // supprimer la colonne qui a été convertie en binaire
        data = data.drop(col)

      }

     // retour de la dataframe transformé
      data

    }

    // définir des colonne pour faire dummy
    val dummyCols = Array[String]("Embarked", "Pclass", "Title", "FamilySize", "FareGroup", "AgeGroup")

    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array[String]("Embarked_Indexed", "Pclass_Indexed", "Title", "FamilySize", "FareGroup", "AgeGroup"))
      .setOutputCols(Array[String]("Embarked_Indexed_Vec", "Pclass_Indexed_Vec", "Title_Vec", "FamilySize_Vec", "FareGroup_Vec", "AgeGroup_Vec"))


    // create output data frame by transforming inout data frame with each function defined above
    // création d'une DF de sortie en transformant la DF inout avec chaque fonction définie ci-dessus
    // NOTE: this will be replaced with a pipeline
    val oneHotModel = oneHotEncoder.fit(
      pclassIndexer(
        embarkedIndexer(
          familySizeCreator(
            titleCreator(
              ageBucketizer(
                fareBucketizer(
                  sexBinerizer(dataFrame=dataFrame
                  )
                )
              )
            )
          )
        )
      )
    )

    val outputData = oneHotModel.transform(
      pclassIndexer(
        embarkedIndexer(
          familySizeCreator(
            titleCreator(
              ageBucketizer(
                fareBucketizer(
                  sexBinerizer(dataFrame=dataFrame
                  )
                )
              )
            )
          )
        )
      )
    )

    // retour de la DF avec les nouvelles fonctionnalités ajoutés
    outputData

  }

}