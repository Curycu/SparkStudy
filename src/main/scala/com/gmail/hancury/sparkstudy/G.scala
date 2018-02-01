package com.gmail.hancury.sparkstudy

// 10. Schema Change
object G extends App {

  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  val spark = SparkSession.builder().appName("G").master("local").getOrCreate()
  import spark.implicits._

  val df = Seq(
    (1, 790101, "F"),
    (2, 820131, "F"),
    (3, 800730, "M"),
    (4, 870426, "F")
  ).toDF("idx", "birth_date", "gender")

  val schema = StructType(
    List(
      StructField("idx_double", DoubleType, false),
      StructField("birth_date_int", IntegerType, false),
      StructField("gender_str", StringType, false)))

  val changeSchema: (DataFrame, StructType) => DataFrame =
    (df, newSchema) => {
      val colNames: Array[String] = df.columns
      val newDf =
        colNames
          .zipWithIndex
          .foldLeft(df) {
            (acc, tu) =>
              val colName = tu._1
              val idx = tu._2
              acc.withColumn(colName, col(colName).cast(newSchema.fields(idx).dataType))
          }
      spark.createDataFrame(newDf.rdd, newSchema)
    }

  val df3 = changeSchema(df, schema)

  df.printSchema
  df.show

  df3.printSchema
  df3.show
}
