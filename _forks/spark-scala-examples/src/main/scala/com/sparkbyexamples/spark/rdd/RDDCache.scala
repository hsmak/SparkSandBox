package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDCache extends App with MyContext {

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  val sc = spark.sparkContext

  val rdd = sc.textFile(s"$data_dir/zipcodes-noheader.csv")

  val rdd2:RDD[ZipCode] = rdd.map(row=>{
    val strArray = row.split(",")
    ZipCode(strArray(0).toInt,strArray(1),strArray(3),strArray(4))
  })

  // Once we have RDD[T] other than RDD[Row] we can create a DataFrame out of it like the following:
  import spark.implicits._
  rdd2.toDF.show

  rdd2.cache()


  println(rdd2.count())
}
