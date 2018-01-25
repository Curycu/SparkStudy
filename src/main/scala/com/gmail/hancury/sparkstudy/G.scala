package com.gmail.hancury.sparkstudy

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// 10. Schema Change
object G extends App {

  val spark = SparkSession.builder().appName("G").master("local").getOrCreate()

  import spark.implicits._

  val df = Seq(
    (1, 790101, "F"),
    (2, 820131, "F"),
    (3, 800730, "M"),
    (4, 870426, "F")
  ).toDF("idx", "birth_date", "gender")

  val newSchema = StructType(
    List(
      StructField("idx", DoubleType, false),
      StructField("birth_date", IntegerType, false),
      StructField("gender", StringType, false)))

  val changeSchema: (DataFrame, StructType) => DataFrame =
    (df, schema) => {
      val colNames = df.columns
      val newDf =
        colNames
          .zipWithIndex
          .foldLeft(df) {
            (acc, tu) =>
              val colName = tu._1
              val idx = tu._2
              acc.withColumn(colName, col(colName).cast(schema.fields(idx).dataType))
          }
      spark.createDataFrame(newDf.rdd, schema)
    }

  val newDf = changeSchema(df, newSchema)

  df.printSchema
  df.show

  newDf.printSchema
  newDf.show
}