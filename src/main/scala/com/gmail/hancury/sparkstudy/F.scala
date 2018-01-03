package com.gmail.hancury.sparkstudy

// 9. Spark basic functions
object F extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().appName("F").master("local").getOrCreate()
  import spark.implicits._
  val sqlContext = spark.sqlContext

  val df =
    Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")
}
