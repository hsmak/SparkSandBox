package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDShuffleExample extends App with MyContext {

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val sc = spark.sparkContext

  val rdd:RDD[String] = sc.textFile(s"$data_dir/test.txt")

  println(rdd.getNumPartitions)
  val rdd2 = rdd.flatMap(f=>f.split(" "))
  .map(m=>(m,1))

  //ReduceBy transformation
  val rdd5 = rdd2.reduceByKey(_ + _)

  println(rdd5.getNumPartitions)

  
}
