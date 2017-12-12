package com.gmail.hancury.sparkstudy

/**
  * Created by cury on 2017-12-11.
  */

object Introduction {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession // import a class

    val spark = SparkSession.builder().appName("Introduction").master("local").getOrCreate()
    val sparkContext = spark.sparkContext

    // 1. read.csv
    spark
      .read.format("csv") // csv format file
      .option("header","true") // use first row as header
      .load("data.csv") // file path
      .show

    // 2. DataFrame with Schema
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType} // import multiple classes

    val schema = StructType(StructField("k", StringType, false) :: StructField("v", IntegerType, true) :: Nil)
    spark
      .createDataFrame(sparkContext.emptyRDD[Row], schema) // set schema
      .printSchema

    // 3. cube & rollup & groupBy
    import spark.implicits._ // import all classes from spark.implicits

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount") // implicit conversion : Seq => DatasetHolder : so we can use .toDF on Seq class

    sales.printSchema // toDF : compare with explicit schema settings (check nullable)
    sales.cube("city","year").count.show // the biggist combination
    sales.rollup("city","year").count.show // the middle size combination
    sales.groupBy("city","year").count.show // the smallest combination
  }
}
