package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateEmptyRDD extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val rdd = spark.sparkContext.emptyRDD
  val rddString = spark.sparkContext.emptyRDD[String]

  println(rdd)
  println(rddString)
  println("Num of Partitions: "+rdd.getNumPartitions)

//  rddString.saveAsTextFile("file:?///tmp/test5.txt")
  //Alternatively
  import spark.implicits._
  rddString
    .toDF
    .write
    .mode(SaveMode.Overwrite)
    .text("file:///tmp/test5.txt")

  val rdd2 = spark.sparkContext.parallelize(Seq.empty[String])
  println(rdd2)
  println("Num of Partitions: "+rdd2.getNumPartitions)

//  rdd2.saveAsTextFile("file:///tmp/test3.txt")
  //Alternatively
  rdd2
    .toDF
    .write
    .mode(SaveMode.Overwrite)
    .text("file:///tmp/test3.txt")


  // Pair RDD

  type dataType = (String,Int)
  var pairRDD = spark.sparkContext.emptyRDD[dataType]
  println(pairRDD)

}
