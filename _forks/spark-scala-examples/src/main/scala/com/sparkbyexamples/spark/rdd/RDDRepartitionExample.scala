package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession

object RDDRepartitionExample extends App with MyContext{

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(Range(0,20))
  println("From local[5]"+rdd.partitions.size)

  val rdd1 = spark.sparkContext.parallelize(Range(0,20), 6)
  println("parallelize : "+rdd1.partitions.size)
  rdd1.partitions.foreach(f=> f.toString)

  val rddFromFile = spark.sparkContext.textFile(s"$data_dir/test.txt",9)
  println("TextFile : "+rddFromFile.partitions.size)
  rdd1.saveAsTextFile(s"$out_dir/RDDRepartitionExample/partition")

  val rdd2 = rdd1.repartition(4)
  println("Repartition size : "+rdd2.partitions.size)
  rdd2.saveAsTextFile(s"$out_dir/RDDRepartitionExample/re-partition")

  val rdd3 = rdd1.coalesce(4)
  println("Repartition size : "+rdd3.partitions.size)

  rdd3.saveAsTextFile(s"$out_dir/RDDRepartitionExample/coalesce")
}
