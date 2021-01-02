package com.sparkbyexamples.spark.dataframe.examples

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object CacheExample extends App with MyContext{

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //read csv with options
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv(s"$data_dir/zipcodes.csv")

  val df2 = df.where(col("State") === "PR")
    .cache()
  df2.show(false)

  println(df2.count())

  val df3 = df2.where(col("Zipcode") === 704)


  println(df2.count())

}
