package df.game

import org.apache.spark.sql.SparkSession

object GameAvgRating {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val inputData = spark.
  }

}
