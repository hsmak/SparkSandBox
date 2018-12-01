package org.hsmak._00_RDD

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object SparkRDDRunner {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SparkRDDRunner")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    val filePath = "file:///home/hsmak/Development/git/spark-sandbox/spark-rdd-dataframe-dataset/src/main/resources/war-and-peace.txt"
    val rdd = sc.textFile(filePath)
      .flatMap(line => line.split(" "))
      .filter(_ contains "Kutuzov")
      .map(w => (w, 1))
      .reduceByKey { (a, b) => a + b }
      .sortByKey(true, 4)

    val dfFromRDD = rdd.toDF("words", "frequency")
    dfFromRDD.show


    // parallel RDD
    val parallelRDD = sc.parallelize(rdd.collect, 10)
    val dfFromParRDD = parallelRDD.toDF("words", "frequency")
    dfFromParRDD.show


    spark.stop()
  }

}
