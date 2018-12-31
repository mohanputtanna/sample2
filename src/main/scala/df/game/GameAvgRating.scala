package df.game
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.explode

object GameAvgRating {

  val spark = SparkSession.builder().appName("STB_Data_Analysis").master("local[2]").getOrCreate()
  import spark.implicits._
  val inputData = spark.read.option("format","com.databricks.spark.csv").
    option("header",true)
    .option("inferSchema",true)
    .csv("/home/mohan/Dataflair/gamedata/ign.csv").cache()

  case class Department(deptId : String, deptName : String)
  case class Employee(firstName : String,lastName : String,email: String,salary : Int)
  case class DepartmentWithEmployee(dept:Department,employees: Seq[Employee])
  case class DepartmentEmployee(dept:Department,empl: Employee)
  case class DepartmentEmployee1(dept1:Department,emp2: Employee)

    val department1 =  Department("00001","Mechanical")
    val department2 =  Department("00002","Computer Science")
    val department3 =  Department("00003","Electrical")
    val department4 =  Department("00004","Civil")

    val department_a = Department("apple","sweet")
    val department_b = Department("apple","sour")
    val department_c = Department("Mango","not_needed")
    val department_d = Department("chiku","not_sweet")

  val dataf1 = Seq(department_a,department_c).toDF()
  val dataf2 = Seq(department_b,department_d).toDF()
//    .withColumnRenamed("deptName","departName")

  val dataf3 = dataf1.join(dataf2,"deptId")

  val dataf4 = dataf3.select("deptId","deptName")
  val dataf5 = dataf3.select("deptId","departName")

  val dataf6 = dataf4.union(dataf5).show()



    val employee1 = new Employee("Mohan","Gowda","mohan@funmail.com",20000)
    val employee2 = new Employee("Vikas","Chauhan","vikas@funmail.com",30000)
    val employee3 = new Employee("Nitin","Yadav","nitinyadav@funmail.com",21000)
    val employee4 = new Employee("Philip","Alexander","philalex@funmail.com",26000)

  val departmentWithEmployee1 = new DepartmentWithEmployee(department1,Seq(employee1,employee2))
  val departmentWithEmployee2 = new DepartmentWithEmployee(department2,Seq(employee3,employee4))
  val departmentWithEmployee3 = new DepartmentWithEmployee(department3,Seq(employee1,employee2))
  val departmentWithEmployee4 = new DepartmentWithEmployee(department4,Seq(employee3,employee4))

  val departmentWithEmployesSeq1 = Seq(departmentWithEmployee1,departmentWithEmployee2)
  val df1 = departmentWithEmployesSeq1.toDF()

  val inputString = "/home/mohan/IdeaProjects/data/parquet"
//  deletePathIfExists(inputString)

    val departmentWithEmployeeSeq2 = Seq(departmentWithEmployee3,departmentWithEmployee4)
    val df2 = departmentWithEmployeeSeq2.toDF()

  def main(args: Array[String]): Unit = {
    //    withColumnExample
//    df1.show(2,false)


//    uniDf.write.parquet("/home/mohan/IdeaProjects/data/parquet")

    val readParquetDf = spark.read.parquet("/home/mohan/IdeaProjects/data/parquet").as[DepartmentWithEmployee]

    readParquetDf.printSchema()
//    readParquetDf.select("dept.deptId","dept.deptName","employees.firstName").show(15,false)
//    readParquetDf.with("dept.deptId","dept.deptName","employees.firstName").show()

    readParquetDf.show(1,false)

    print("Number of elements => " + readParquetDf.count())
    readParquetDf.flatMap(rec => rec.employees
        .map(emp => (emp.firstName,emp.lastName,emp.email,emp.salary))
    )
//      .show()

    val expDFCor = readParquetDf.withColumn("employeesData",explode($"employees")).drop($"employees")
    expDFCor.printSchema()


    val explodedDF = readParquetDf.explode($"employees"){
      case Row(employee : Seq[Row]) => employee
        .map(employee =>{
        val firstName = employee(0).asInstanceOf[String]
        val lastName = employee(1).asInstanceOf[String]
        val email = employee(2).asInstanceOf[String]
        val salary = employee(3).asInstanceOf[Int]
      Employee(firstName,lastName,email,salary)}
        )
    }.cache()
//    explodedDF.show(1,false)
    //    print(readParquetDf:Any)
    //    readParquetDf.flatMap{
    //      row:Row => row.getString("Depart")
    //
    //    }

    val ds = Seq(
      (0, "Lorem ipsum dolor", 1.0, Array("prp1", "prp2", "prp3")))
      .toDF("id", "text", "value", "properties")
      .as[(Integer, String, Double, scala.List[String])]
    print(ds:Any)
    ds.flatMap { t =>
      t._4.map { prp =>
        (t._1, t._2, t._3, prp) }}
//      .show


    val jsonString = """
  {
  "url": "imap.yahoo.com",
  "username": "myusername",
  "password": "mypassword"
  }
  """
    val jsonString25 = """
  {
  "url": "imap.yahoo.com",
  "username": "myusername",
  "password": "mypassword"
  }
  """
    val jsonString1 = """
  {
  "url": "imap.yahoo.com",
  "username": "myusername",
  "password": "mypassword"
  }
  """
    val jsonString2 = """
  "url": "pop.yahoo.com",
  "username": "username2",
  "password": "mypassword2"
  }
  """
    val jsonString3 = """
  {
  "url": "jop.yahoo.com",
  "username": "username3",
  "password": "mypassword3"
  }
  """
    val jsonString5 = """
  {
  "url": "fop.yahoo.com",
  "username": "username5",
  "password": "mypassword5"
  }
  """


    val inDf = List((1,"James",jsonString),(2,"Kanye",jsonString1)).toDF("id","name","web")

  }

  def exceptDf(df1:DataFrame,df2:DataFrame):DataFrame ={
    df1.except(df2)
    df1.union(df2)

  }

  def withColumnExample = {
    val newDf = inputData.withColumn("absRating",when($"score" > 9 and $"score".isNotNull,"Good")
      .otherwise("Not so good"))
      .drop(col("score_phrase"))
    newDf.take(5).foreach(println)
    val newDf1 = newDf.withColumn("negAbsRating",negate(col("score")))
//      .show(5)
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
