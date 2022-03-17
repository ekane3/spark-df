package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrame").master("local[*]").getOrCreate()
    //val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/codesPostaux.csv")
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true","delimiter" ->";")).csv("src/main/resources/codesPostaux.csv")
    
    // Montrons un apercu de notre DataFrame
    print("\n \n ************* Apercu du DataFrame ************* \n \n")
    df.show()

     // Quel est le schéma du fichier ?
    print("\n \n ************* Schéma du DataFrame ************* \n \n")
    df.printSchema()

    // Afficher le nombre de communes
    print("\n \n ************* Nombre de communes ************* \n \n")
    print("Nombre : ",df.select("Nom_commune").distinct().count())

    // Nombre de communes qui possèdent l'attribut Ligne_5
    print("\n \n ************* Nombre de communes qui possèdent l'attribut Ligne_5 ************* \n \n")
    print("Nombre : ",df.filter(df("Ligne_5").isNotNull).select("Nom_commune").distinct().count())

    // Ajoutons aux données une colonne contenant le numéro de département de la commune
    print("\n \n ************* Ajoutons aux données une colonne contenant le numéro de département de la commune ************* \n \n")
    val df2 = df.withColumn("departement", df("Code_postal").substr(0,2))
    df2.show()

    // Ecrivons le résultat dans un nouveau fichier csv nommé "commune_et_departement.csv", ayant pour colonne :
    // Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
    df2.select("Code_commune_INSEE", "Nom_commune", "Code_postal", "departement").orderBy("Code_postal").write.format("csv").option("header", "true").save("src/main/resources/commune_et_departement.csv")
    
    // Affichons les communes du département de l'Aisne
    print("\n \n ************* Les communes du département de l'Aisne ************* \n \n")
    df2.select("Nom_commune").filter("departement = '02'").show()

    // Affichons le département avec le plus de communes
    print("\n \n ************* Le département avec le plus de communes ************* \n \n")
    df2.groupBy("departement").count().orderBy(desc("count")).show()
    
  }

  }

