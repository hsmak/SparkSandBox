package org.hsmak._00_RDD

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object SparkRDDRunner {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("SparkRDDRunner")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    val filePath = "file:///home/hsmak/Development/git/spark-sandbox/_data/test.csv"
    val rdd = sc.textFile(filePath)
      .flatMap(line => line.split(","))
      .filter(_ contains "Gilbert")
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
