package com.gmail.hancury.sparkstudy

object B extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().appName("B").master("local").getOrCreate()
  import spark.implicits._
  val sqlContext = spark.sqlContext

  // 4. sqlContext.sql : query base approach
  Seq(
    ("romance", 2016, 100),
    ("romance", 2017, 200),
    ("general", 2015, 50),
    ("general", 2016, 150),
    ("bl", 2017, 50)
  ).toDF("genre", "year", "amount").createTempView("sales_data")

  val dynamicQuery: (String, Int) => String = // dynamic query : by string interpolation & function
    (genre, year) => s"select * from sales_data where genre = '$genre' and year = '$year'"

  sqlContext.sql(dynamicQuery("general", 2016)).show
}
