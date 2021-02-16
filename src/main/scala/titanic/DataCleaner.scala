package titanic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{mean, round}

// nettoyage des données brutes ici - formattage des colonnes, remplacement des valeurs manquantes, etc.

object DataCleaner {

  // fonction de production d'une données propre à partir de données brut
  def cleanData(dataFrame: DataFrame): DataFrame = {

    // fonction de formattage correcte des doonnées
    def formatData(dataFrame: DataFrame): DataFrame = {

      dataFrame
        .withColumn("Survived", dataFrame("Survived").cast("Int"))
        .withColumn("Age", dataFrame("Age").cast("Double"))
        .withColumn("Fare", dataFrame("Fare").cast("Double"))
        .withColumn("Parch", dataFrame("Parch").cast("Double"))
        .withColumn("SibSp", dataFrame("SibSp").cast("Double"))
        .withColumnRenamed("Survived", "label")

    }


    // fonction de calcul de la moyenne de la colonne âge
    def meanAge(dataFrame: DataFrame): Double = {

      // sélectionne la colonne d'âge et calcule l'âge moyen à partir des valeurs non-NA
      dataFrame
        .select("Age")
        .na.drop()
        .agg(round(mean("Age"), 0))
        .first()
        .getDouble(0)

    }

    // fonction pour calculer le tarif moyen
    def meanFare(dataFrame: DataFrame): Double = {

      // sélectionne la colonne tarif et calcul le tarif moyen à partir des valeurs non-NA
      dataFrame
        .select("Fare")
        .na.drop()
        .agg(round(mean("Fare"), 2))
        .first()
        .getDouble(0)

    }

    // formattage des données brutes
    val formattedData = formatData(dataFrame)

    // Remplacement des valeurs manquantes pour l'âge, le tarif et embarqué et supprimez les colonnes inutiles
    val outputData = formattedData
      .na.fill(Map(
      "Age" -> meanAge(formattedData),
      "Fare" -> meanFare(formattedData),
      "Embarked" -> "S"
    ))
      .drop("Ticket", "Cabin")

    // return cleaned data frame
    outputData

  }

}