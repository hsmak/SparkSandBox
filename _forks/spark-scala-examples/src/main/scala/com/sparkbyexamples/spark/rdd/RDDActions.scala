package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.rdd.OperationOnPairRDDComplex.kv
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object RDDActions extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val pairRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

  val intRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  //Collect
  val data:Array[Int] = intRdd.collect()
  data.foreach(println)

  //aggregate
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+intRdd.aggregate(0)(param0,param1))
  //Output: aggregate : 20

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+pairRDD.aggregate(0)(param3,param4))
  //Output: aggregate : 20

  //treeAggregate. This is similar to aggregate
  def param8= (accu:Int, v:Int) => accu + v
  def param9= (accu1:Int,accu2:Int) => accu1 + accu2
  println("treeAggregate : "+intRdd.treeAggregate(0)(param8,param9))
  //Output: treeAggregate : 20

  //fold
  println("fold :  "+intRdd.fold(0){ (acc, v) =>
    val sum = acc+v
    sum
  })
  //Output: fold :  20

  println("fold :  "+pairRDD.fold(("Total",0)){ (acc:(String,Int), v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  //Output: fold :  (Total,181)

  //reduce
  println("reduce : "+intRdd.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : "+intRdd.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : "+pairRDD.reduce((x, y) => ("Total",x._2 + y._2)))
  //Output: reduce : (Total,181)

  //treeReduce. This is similar to reduce
  println("treeReduce : "+intRdd.treeReduce(_ + _))
  //Output: treeReduce : 20

  //count, countApprox, countApproxDistinct
  println("Count : "+intRdd.count)
  //Output: Count : 20
  println("countApprox : "+intRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : "+intRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : "+pairRDD.countApproxDistinct())
  //Output: countApproxDistinct : 5

  //countByValue, countByValueApprox
  println("countByValue :  "+intRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox())

  //first
  println("first :  "+intRdd.first())
  //Output: first :  1
  println("first :  "+pairRDD.first())
  //Output: first :  (Z,1)

  //top
  println("top : "+intRdd.top(2).mkString(","))
  //Output: take : 5,4
  println("top : "+pairRDD.top(2).mkString(","))
  //Output: take : (Z,1),(C,40)

  //min
  println("min :  "+intRdd.min())
  //Output: min :  1
  println("min :  "+pairRDD.min())
  //Output: min :  (A,20)

  //max
  println("max :  "+intRdd.max())
  //Output: max :  5
  println("max :  "+pairRDD.max())
  //Output: max :  (Z,1)

  //take, takeOrdered, takeSample
  println("take : "+intRdd.take(2).mkString(","))
  //Output: take : 1,2
  println("takeOrdered : "+ intRdd.takeOrdered(2).mkString(","))
  //Output: takeOrdered : 1,2
  //println("take : "+listRdd.takeSample())

  //toLocalIterator
  //listRdd.toLocalIterator.foreach(println)
  //Output:

}
