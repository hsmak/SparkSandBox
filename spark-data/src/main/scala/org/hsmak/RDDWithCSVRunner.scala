package org.hsmak

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object RDDWithCSVRunner {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("RDDWithCSV")
      .getOrCreate()

    // Retrieve SparkContext from SparkSession
    val sc = spark.sparkContext



    val filePath = s"file://${System.getProperty("user.dir")}/_data/fdps-v3-master/data/Line_of_numbers.csv"
    val inFile = sc.textFile(filePath);

    val splitLines = inFile.map(line => {
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    })

    val numericData = splitLines.map(line => line.map(_.toDouble))
    val summedData = numericData.map(row => row.sum)

    println(summedData.collect().mkString(","))

    spark.stop()
  }

}
