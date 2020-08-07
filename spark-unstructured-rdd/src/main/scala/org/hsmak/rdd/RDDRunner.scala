package org.hsmak.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDRunner {

  Logger.getLogger("org").setLevel(Level.OFF)

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




    // TODO - to be tested
    import scala.io.Source._

//    read a json file from a url stream
    val json = fromURL("https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2010-summary.json#")
      .mkString
      .split("\n")// since it's a multi-line json


    // convert Json string to RDD
    val rddJson = sc.parallelize(json, 8)
    //convert jsonRDD -> DS -> DF
    val df = spark.read.option("header", true).option("inferSchema", true).json(rddJson.toDS)


    import org.apache.spark.sql.functions._
    df.groupBy('DEST_COUNTRY_NAME).agg(sum('count) as 'sum).select("*").sort('sum desc).show

    df.createOrReplaceTempView("flights")

    spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) as sum FROM flights GROUP BY DEST_COUNTRY_NAME ORDER BY sum DESC LIMIT 5").show
    spark.stop()
  }

}
