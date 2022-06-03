package BigData.DataWriter

import BigData.Case.Rentals
import org.apache.spark.sql.{Dataset, SparkSession}

object SaveFile {
  def saveRentals(spark: SparkSession, path: String, fileName: String, dataset: Dataset[Rentals]) = {
    dataset.write.format("csv").option("header",true).save(path+"//"+fileName)
  }
}
