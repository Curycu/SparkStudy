package com.gmail.hancury.sparkstudy

// 11. cbind : columnar union DataFrame to DataFrame
object H extends App {

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  val spark = SparkSession.builder().appName("H").master("local").getOrCreate()
  import spark.implicits._

  val cbind: (DataFrame, DataFrame) => DataFrame =
    (df, df2) => {
      val x =
        spark
          .createDataFrame(
            df.rdd.zipWithUniqueId.map(tu => Row(tu._1.toSeq.:+(tu._2): _*)),
            df.schema.add(StructField("primaryKeyForCbind", LongType, false)))
          .withColumn("orderKeyForCbind", $"primaryKeyForCbind")
          .as("df")
      val y =
        spark
          .createDataFrame(
            df2.rdd.zipWithUniqueId.map(tu => Row(tu._1.toSeq.:+(tu._2): _*)),
            df2.schema.add(StructField("primaryKeyForCbind", LongType, false)))
          .as("df2")
      x.join(y, col("df.primaryKeyForCbind") === col("df2.primaryKeyForCbind"))
        .sort("orderKeyForCbind")
        .drop("primaryKeyForCbind", "orderKeyForCbind")
    }

  val df =
    Seq(
      ("curycu", "2018-01-01"),
      ("curycu", "2018-01-07"),
      ("curycu", "2018-01-12"),
      ("curycu", "2018-01-15"),
      ("tester", "2018-01-01"),
      ("tester", "2018-01-11"),
      ("tester", "2018-01-18"))
      .toDF("id", "date")

  val df2 = Seq(0, 6, 5, 3, 0, 10, 7).toDF("daygap")

  val res = cbind(cbind(cbind(df, df2), df2), df)
  res.printSchema
  res.show
}
