package org.hsmak.rddOps

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDSetOps {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("RDDSetOps")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    spark.stop()
  }

}
