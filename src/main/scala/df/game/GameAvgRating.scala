package df.game

import org.apache.spark.sql.SparkSession

object GameAvgRating {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("STB_Data_Analysis").master("local[2]").getOrCreate()
    val inputData = spark.read.csv("/home/mohan/Dataflair/gamedata/ign.csv")
    inputData.show(10)
  }

}
