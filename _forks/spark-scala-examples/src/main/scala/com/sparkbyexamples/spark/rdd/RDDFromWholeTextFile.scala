package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDFromWholeTextFile extends MyContext{

  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.wholeTextFiles(s"$data_dir/txt/alice.txt")
    rdd.foreach(a=>println(a._1+"---->"+a._2))

  }
}
