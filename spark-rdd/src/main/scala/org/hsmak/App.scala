package org.hsmak

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))


    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()

    import spark.implicits._

    // via SparkSQL
    val dataset = spark.read.textFile("file:///home/hsmak/Development/git/spark-sandbox/spark-rdd/src/main/resources/war-and-peace.txt").flatMap(line => line.split(" ")).map(w => (w, 1))
    dataset.show()

    val df = dataset.toDF("words", "frequency")
    df.show()


    //////////////////////////


    // via RDD
    val sc = spark.sparkContext
    val rdd = sc.textFile("file:///home/hsmak/Development/git/spark-sandbox/spark-rdd/src/main/resources/war-and-peace.txt").flatMap(line => line.split(" ")).filter(_ contains "Kutuzov").map(w => (w, 1)).reduceByKey { (a, b) => a + b }.sortByKey(true, 4)
    val dfFromRDD = rdd.toDF("words", "frequency")
    dfFromRDD.show


    // parallel RDD
    val parallelRDD = sc.parallelize(rdd.collect, 10)
    val dfFromParRDD = parallelRDD.toDF("words", "frequency")
    dfFromParRDD.show


    spark.stop()
  }

}
