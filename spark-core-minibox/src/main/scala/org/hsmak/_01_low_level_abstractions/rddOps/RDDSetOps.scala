package org.hsmak._01_low_level_abstractions.rddOps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDSetOps {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RDDSetOps")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    spark.stop()
  }

}
