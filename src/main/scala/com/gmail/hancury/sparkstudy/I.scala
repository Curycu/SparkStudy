package com.gmail.hancury.sparkstudy

// 12. window : diff, ms3, cumsum
object I extends App {

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.expressions.Window
  import scala.util._
  import java.time.LocalDate
  import java.time.temporal.ChronoUnit

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

  val w = Window.partitionBy("id").orderBy("date")

  df
    .withColumn("rank", rank().over(w))
    .filter(expr("rank <= 2"))
    .select("id", "date", "rank")
    .show()

  val dayGap = (aft: String, bef: String) => {
    val befDate = Try(LocalDate.parse(bef))
    val aftDate = Try(LocalDate.parse(aft))

    val triedRes =
      for {
        aft <- aftDate
        bef <- befDate
      } yield ChronoUnit.DAYS.between(aft, bef)

    triedRes match {
      case Success(x) => x.toInt
      case _ => 0
    }
  }

  val dayGapUdf = udf[Int, String, String](dayGap)

  df
    .withColumn("lag_date", lag($"date", 1).over(w))
    .withColumn("daygap", dayGapUdf($"lag_date", $"date"))
    .select("id", "date", "lag_date", "daygap")
    .show()

  val wms3 = Window.partitionBy("id").orderBy("date").rowsBetween(-1, 1)

  df
    .withColumn("ms3_amount", sum($"amount").over(wms3))
    .select("id", "amount", "ms3_amount")
    .show()

  val wcumsum = Window.partitionBy("id").orderBy("date").rowsBetween(Long.MinValue, 0)

  df
    .withColumn("cumsum_amount", sum($"amount").over(wcumsum))
    .select("id", "amount", "cumsum_amount")
    .show()
}
