package com.gmail.hancury.sparkstudy

object Second extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().appName("Second").master("local").getOrCreate()
  import spark.implicits._
  val sqlContext = spark.sqlContext

  // 4. sqlContext.sql : query base approach
  Seq(
    ("Warsaw", 2016, 100),
    ("Warsaw", 2017, 200),
    ("Boston", 2015, 50),
    ("Boston", 2016, 150),
    ("Toronto", 2017, 50)
  ).toDF("city", "year", "amount").createTempView("sales_data")

  val dynamicQuery: (String, Int) => String =
    (city, year) => s"select * from sales_data where city = '$city' and year = '$year'"

  sqlContext.sql(dynamicQuery("Boston", 2016)).show
}
