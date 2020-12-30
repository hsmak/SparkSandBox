package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadMultipleCSVFiles extends App with MyContext{

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile(s"$data_dir/csv/text01.txt")
  println(rddFromFile.getClass)
  println

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  println("Iterate RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println(rdd)
  println

  println("Get data Using collect")
  rdd.collect().foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println

  println("read all csv files from a directory to single RDD")
  val rdd2 = spark.sparkContext.textFile(s"$data_dir/csv/*")
  rdd2.foreach(f=>{
    println(f)
  })
  println

  println("read csv files base on wildcard character")
  val rdd3 = spark.sparkContext.textFile(s"$data_dir/csv/text*.txt")
  rdd3.foreach(f=>{
    println(f)
  })
  println

  println("read multiple csv files into a RDD")
  val rdd4 = spark.sparkContext.textFile(s"$data_dir/csv/text01.txt,$data_dir/csv/text02.txt")
  rdd4.foreach(f=>{
    println(f)
  })
  println

}

