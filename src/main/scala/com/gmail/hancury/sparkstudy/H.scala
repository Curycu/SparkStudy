package com.gmail.hancury.sparkstudy

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// 11. calculate purchase day-gap of users
object H extends App {
  
  val spark = SparkSession.builder().appName("G").master("local").getOrCreate()
  import spark.implicits._

  val df = Seq(
    ("curycu", "20170105120030"),
    ("curycu", "20170101012045"),
    ("curycu", "20170102234510"),
    ("tester", "20170104120030"),
    ("tester", "20170101022142"),
    ("tester", "20170109132540"))
    .toDF(Seq("uid", "buy_date"): _*)
    .withColumn("buy_date", to_date(col("buy_date"), "yyyyMMddHHmmss").cast(StringType))
    .orderBy(Seq($"uid", $"buy_date"): _*)

  df.printSchema
  df.show

  val res =
    df.rdd
      .map(r => (r.getString(0), r.getString(1)))
      .aggregateByKey(Seq[String]())(_ :+ _, _ ++ _)
      .toDF(Seq("uid", "buy_date_seq"): _*)

  res.printSchema
  res.show
}
