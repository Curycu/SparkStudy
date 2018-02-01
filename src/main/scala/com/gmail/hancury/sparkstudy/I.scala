package com.gmail.hancury.sparkstudy

// 12. diffColByKey
object I extends App {
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import scala.reflect.ClassTag
  import scala.reflect.runtime.universe.TypeTag
  import java.text.SimpleDateFormat

  val spark = SparkSession.builder().appName("sparksql").master("local").getOrCreate()
  import spark.implicits._

  val df =
    Seq(
      ("curycu", "2018-01-01", 10),
      ("curycu", "2018-01-07", 3),
      ("curycu", "2018-01-12", 7),
      ("curycu", "2018-01-15", 2),
      ("tester", "2018-01-01", 1),
      ("tester", "2018-01-11", 3),
      ("tester", "2018-01-18", 10))
      .toDF("id", "date", "amount")

  def diffColByKey[A: TypeTag: ClassTag, B: TypeTag]
  (df: DataFrame, key: String, target: String, diffFunc: (A, A) => B, zero: B) = {

    val keyType =
      df.select(key).schema.fields.apply(0).dataType

    val diffSeq: Seq[A] => Seq[B] =
      xs => {
        if (xs.length < 2) Seq(zero)
        else xs.tail.zipWithIndex.map { tu =>
          val x2 = tu._1
          val idx: Int = tu._2
          val x1 = xs.init(idx)
          diffFunc(x2, x1)
        }.+:(zero)
      }

    val funcUdf = udf(diffSeq)

    val resultDf: DataFrame =
      df.select(key, target)
        .rdd
        .map(row => (row.getString(0), row.getAs[A](1)))
        .aggregateByKey(Seq[A]())(_ :+ _, _ ++ _)
        .toDF(key, target)
        .withColumn("diff_" + target, funcUdf(col(target)))
        .withColumn(key, col(key).cast(keyType))

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

    cbind(
      resultDf.select(col(key), explode(col(target)).as(target)),
      resultDf.select(explode(col("diff_" + target)).as("diff_" + target)))
  }

  val res = diffColByKey(df, "id", "amount", (x: Int, y: Int) => x - y, 0)
  res.printSchema
  res.show

  val timegap: (SimpleDateFormat, Int) => (String, String) => Int =
    (sdf, by) => (t2, t1) => ((sdf.parse(t2).getTime - sdf.parse(t1).getTime) / by).toInt

  val daygap: (String, String) => Int =
    timegap(new SimpleDateFormat("yyyy-MM-dd"), 1000 * 60 * 60 * 24)

  val res2 = diffColByKey(df, "id", "date", daygap, 0)
  res2.printSchema
  res2.show
}
