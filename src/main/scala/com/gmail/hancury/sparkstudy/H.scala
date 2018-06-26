package com.gmail.hancury.sparkstudy

// 11. udf
object H extends App {

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import java.time.LocalDate

  val spark = SparkSession.builder().appName("sparksql").master("local").getOrCreate()
  import spark.implicits._

  val df =
    Seq(
      ("A", "861117"),
      ("B", "830325"),
      ("C", "160126"),
      ("D", "620603"))
      .toDF("id", "birth_date")

  val getAge: String => Int = birthDate => {
    val nowYear = LocalDate.now().getYear().toString.substring(2,4).toInt
    val birthYear = birthDate.substring(0,2).toInt
    if(birthYear <= nowYear) nowYear - birthYear + 1
    else 100 - birthYear + nowYear + 1
  }
  val getAgeUdf = udf[Int, String](getAge)

  df
    .filter(expr("birth_date is not null"))
    .withColumn("age", getAgeUdf(col("birth_date")))
    .show()
}
