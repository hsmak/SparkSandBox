package com.sparkbyexamples.spark.dataframe.functions.datetime

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, to_timestamp}

/**
  * Doesn't work with Spark 3
  */
object DayWeekAndWeekMonth extends App with MyContext {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df = Seq(("2019-07-01 12:01:19.000"),
    ("2019-06-24 12:01:19.000"),
    ("2019-11-16 16:44:55.406"),
    ("2019-11-16 16:50:59.406")).toDF("input_timestamp")

  df.withColumn("input_timestamp",
    to_timestamp(col("input_timestamp")))
    .withColumn("week_day_number", date_format(col("input_timestamp"), "u"))
    .withColumn("week_day_abb", date_format(col("input_timestamp"), "E"))
    .show(false)

  df.withColumn("input_timestamp",
    to_timestamp(col("input_timestamp")))
    .withColumn("week_day_full", date_format(col("input_timestamp"), "EEEE"))
    .withColumn("week_of_month", date_format(col("input_timestamp"), "W"))
    .show(false)
}