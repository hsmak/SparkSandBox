package org.hsmak._02_high_level_abstractions.dataframes

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object DataFrameRunner {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("DataFrameRunner")
      .getOrCreate()



    spark.stop()
  }

}
