package BigData

import BigData.DataReader.ReadFile
import org.apache.spark.sql.SparkSession

object Main{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Project")
      .getOrCreate()

    // reading files
    val rentalsDf = ReadFile.readRentals(spark, args(0))
    val actorsDf = ReadFile.readActors(spark, args(1))

    //filtering files
    val actors_filteredDf = actorsDf.filter(row=>BigData.Transformation.Transformations.isActorWithCharacters(row))
    println("Actors with given characters:")
    actors_filteredDf.show()

    println("Rentals on weekends:")
    val rentals_filteredDf = rentalsDf.filter(row=>BigData.Transformation.Transformations.isNotWorkingday(row))
    rentals_filteredDf.show()

    //saving filtered rentals
    DataWriter.SaveFile.saveRentals(spark, args(2), "rentals_on_weekends", rentals_filteredDf)
  }
}
