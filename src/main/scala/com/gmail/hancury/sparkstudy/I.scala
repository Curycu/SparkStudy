package com.gmail.hancury.sparkstudy

// 12. diffByKey
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

def diffByKey[A: TypeTag: ClassTag, B: TypeTag]
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

  val resultDf =
    df.select(key, target)
      .rdd // (key, value)
      .zipWithIndex // ((key, value), index)
      .map(t => (t._1.getString(0), (t._1.getAs[A](1), t._2))) // (key, (value, index))
      .aggregateByKey(Seq[(A, Long)]())((seq, elem) => seq :+ elem, (mat, seq) => mat ++ seq) // (key, Seq[(value, index)])
      .map {t =>
        val sortedValue = t._2.sortBy(_._2).map(_._1)
        (t._1, sortedValue) // (key, Seq[value])
      }
      .toDF(key, target)
      .withColumn("diff_" + target, funcUdf(col(target))) // (key, Seq[value], diffSeq[value])
      .withColumn(key, col(key).cast(keyType))

  val cbind: (DataFrame, DataFrame) => DataFrame =
    (df, df2) => {
      val x =
        spark
          .createDataFrame(
            df.rdd.zipWithIndex.map(tu => Row(tu._1.toSeq.:+(tu._2): _*)),
            df.schema.add(StructField("primaryKeyForCbind", LongType, false)))
          .withColumn("orderKeyForCbind", $"primaryKeyForCbind")
          .as("df")
      val y =
        spark
          .createDataFrame(
            df2.rdd.zipWithIndex.map(tu => Row(tu._1.toSeq.:+(tu._2): _*)),
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

val res = diffByKey(df, "id", "amount", (x: Int, y: Int) => x - y, 0)
res.show

val timegap: (SimpleDateFormat, Int) => (String, String) => Double =
  (sdf, by) => (t2, t1) => (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / by

val daygap: (String, String) => Double =
  timegap(new SimpleDateFormat("yyyy-MM-dd"), 1000 * 60 * 60 * 24)

val res2 = diffByKey(df, "id", "date", daygap, 0.0)
res2.show
}
