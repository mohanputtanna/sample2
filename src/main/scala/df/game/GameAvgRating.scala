package df.game

import org.apache.spark.sql.SparkSession

object GameAvgRating {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("STB_Data_Analysis").master("local[2]").getOrCreate()
    import spark.implicits._
    val inputData = spark.read.option("format","com.databricks.spark.csv").
      option("header",true).
      csv("/home/mohan/Dataflair/gamedata/ign.csv")
    inputData.createTempView("GameData")
    spark.sql("select * from GameData where title like '%Steins%'").show()
    val inputDataRDD = inputData.rdd
    val accum = spark.sparkContext.longAccumulator("Not a string accumulator")

    try {
      val inputRDDMap = inputDataRDD.filter(rec =>
        {
          if(!rec.getString(5).matches("[0-9]*[.]?[0-9]*"))
            accum.add(1)
          rec.getString(5).matches("[0-9]*[.]?[0-9]*")
        }).
        map(rec =>((rec.getString(2)),(rec.getString(5).toFloat,1))).
        reduceByKey((acc,value) => (acc._1+value._1,acc._2+1)).
        map(rec => (rec._1,rec._2._1/rec._2._2)).
        take(10).foreach(println)
    }catch {
      case e: NumberFormatException => println("Cannot convert the string to number")
      case e: Exception => e.printStackTrace()
    }
    print(accum)
  }
  def isAllDigits(x: String):Boolean = x.matches("[0-9]*[.]?[0-9]*")

}
