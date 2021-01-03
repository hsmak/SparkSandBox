package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CurrentDateAndTime extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  //Get current Date & Time
  val df = Seq(1).toDF("seq") // we can use spark.sql() directly instead of this

  val curDate = df
    .withColumn("current_date",current_date().as("current_date"))
    .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
  curDate.show(false)

  spark.sql(
    """
      |select 1 as id, current_date() as current_date, current_timestamp() as current_timestamp""".stripMargin).show(false)


  curDate.select(
    date_format(col("current_timestamp"),"MM-dd-yyyy").as("date"),
    date_format(col("current_timestamp"),"HH:mm:ss.SSS").as("time"),
    date_format(col("current_date"), "MM-dd-yyyy").as("current_date_formateed"))
    .show(false)

  spark.sql(
    """
      |select
      |   current_timestamp,
      |   date_format(current_timestamp, 'MM-dd-yyyy') as date,
      |   date_format(current_timestamp, 'HH:mm:ss.SSS') as time,
      |   date_format(current_date, 'MM-dd-yyyy') as current_date_formatted
      |FROM (select 1 as id, current_date() as current_date, current_timestamp() as current_timestamp)""".stripMargin).show(false)

}
