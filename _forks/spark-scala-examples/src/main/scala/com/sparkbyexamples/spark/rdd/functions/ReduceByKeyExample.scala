package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object ReduceByKeyExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(("Project", 1),
  ("Gutenberg’s", 1),
  ("Alice’s", 1),
  ("Adventures", 1),
  ("in", 1),
  ("Wonderland", 1),
  ("Project", 1),
  ("Gutenberg’s", 1),
  ("Adventures", 1),
  ("in", 1),
  ("Wonderland", 1),
  ("Project", 1),
  ("Gutenberg’s", 1))

  val rdd=spark.sparkContext.parallelize(data)

  val rdd2=rdd.reduceByKey(_ + _)

  rdd2.foreach(println)

  /*
   * Complete Example
   */
  val seqOfStr = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")
  val rddOfStr = spark.sparkContext.parallelize(seqOfStr)
  rddOfStr.flatMap(s => s.split(" "))
    .map(w => (w, 1))
    .reduceByKey(_+_).foreach(println)
}
