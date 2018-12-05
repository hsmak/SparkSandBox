package org.hsmak._00_RDD

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDWithCSV {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("RDDWithCSV")
      .getOrCreate()

    import spark.implicits._


    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext


    val filePath = "file:////home/hsmak/Development/git/spark-sandbox/_data/fdps-v3-master/data/Line_of_numbers.csv"

    val numberRDDs = sc.textFile(filePath)
      .flatMap(line => line.split(",").map(_.toDouble))

    //Note: println() will work only in a Local Mode for the Program Driver will do the Reduction + Printing
    println(numberRDDs.collect.toList)
    println(numberRDDs.sum)

    spark.stop()
  }

}
