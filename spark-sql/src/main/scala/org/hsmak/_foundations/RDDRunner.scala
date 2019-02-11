package org.hsmak._foundations

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDRunner {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("RDDRunner")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"
    val rdd = sc.textFile(filePath)
      .flatMap(line => line.split(","))
      .filter(_ contains "Gilbert")
      .map(w => (w, 1)) // Mapping to a Tuple2 will yield a Key-Value HashMap; Hence we can use reduceByKey() as follows
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
