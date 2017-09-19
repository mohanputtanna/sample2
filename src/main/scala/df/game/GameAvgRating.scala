package df.game

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object GameAvgRating {

  val spark = SparkSession.builder().appName("STB_Data_Analysis").master("local[2]").getOrCreate()
  import spark.implicits._
  val inputData = spark.read.option("format","com.databricks.spark.csv").
    option("header",true)
    .option("inferSchema",true)
    .csv("/home/mohan/Dataflair/gamedata/ign.csv").cache()

  def main(args: Array[String]): Unit = {


withColumnExample
  }

  def withColumnExample = {
    val newDf = inputData.withColumn("absRating",when($"score" > 9 and $"score".isNotNull,"Good")
      .otherwise("Not so good"))
      .drop(col("score_phrase"))
    newDf.take(5).foreach(println)
    val newDf1 = newDf.withColumn("negAbsRating",negate(col("score"))).show(5)

  }

  def isAllDigits(x: String):Boolean = x.matches("[0-9]*[.]?[0-9]*")

  def dollarExample = {
    inputData.select($"score"+1)
      .show(5)
    inputData.groupBy("release_year")
      .count()
      .orderBy("release_year")
      .show()
  }

  def withColumnRenamedExample={
    inputData.withColumnRenamed("_c0","serialNo")
      .select("title","score")
      .groupBy("title")
      .avg("score")
      .show(5)
  }

  def kpi1 ={
    val inputDataRDD = inputData.rdd
    inputDataRDD.map(rec => ((rec.getAs[Int](9),rec.getAs[Int](8)),rec.getString(2)))
      .groupByKey().
      mapValues(rec => (rec.toList))
      .take(5)
      .foreach(println)
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
        map(rec => (rec._1,rec._2._1/rec._2._2))
      //        .take(10).foreach(println)
    }catch {
      case e: NumberFormatException => println("Cannot convert the string to number")
      case e: Exception => e.printStackTrace()
    }
    print(accum)

  }
}
