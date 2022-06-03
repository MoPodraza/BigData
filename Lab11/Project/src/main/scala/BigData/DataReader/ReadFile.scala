package BigData.DataReader

import BigData.Case.{Actors, Rentals}
import org.apache.spark.sql.{Dataset, SparkSession}


object ReadFile {
  def readRentals(spark: SparkSession, path: String): Dataset[Rentals] = {
    import spark.implicits._
    return spark.read.format("csv").option("header", true).load(path).as[Rentals]
  }
  def readActors(spark: SparkSession, path: String): Dataset[Actors] = {
    import spark.implicits._
    return spark.read.format("csv").option("header", true).load(path).as[Actors]
  }

}
