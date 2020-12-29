package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object aggregateExample extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //aggregate example
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2)) // RDD[Int]

  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("output 1 : " + listRdd.aggregate(0)(param0,param1))
  //in one shot
  println("output 1 (in one shot) : " + listRdd.aggregate(0)((acc, nxt) => acc+nxt, (acc1, acc2) => acc1+acc2)) // similar to foldLeft
  println

  /*
   * Example 2
   */
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60))) //RDD[Tuple2[String, Int]]
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("output 2 : " + inputRDD.aggregate(0)(param3, param4))
  println("output 2 (in one shot) : " + inputRDD.aggregate(0)((acc, nxt) => acc + nxt._2, (acc1, acc2) => acc1 + acc2))
  println

  /*
   * Observations:
   *    - Each partition got an initial value of zero_value
   *    - Hence, (total_accumulation = number_of_partitions * zero_value + zero_value)
   */
  println("Number fo Partitions :"+listRdd.getNumPartitions)
  //aggregate example
  println("output 1 : "+listRdd.aggregate(1)(param0,param1))

}
