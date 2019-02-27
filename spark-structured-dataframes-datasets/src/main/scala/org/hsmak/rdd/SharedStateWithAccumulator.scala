package org.hsmak.rdd

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.SparkSession

object SharedStateWithAccumulator {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("AccumulatorRunner")
      .getOrCreate()

    val sc = spark.sparkContext

    println(s"Running Spark Version ${sc.version}")

    //Deprecated
    //    val invalidLineCounter = sc.accumulator(0);
    //    val invalidNumericLineCounter = sc.accumulator(0);

    val invalidLineCounter = sc.longAccumulator("invalidLineCounter")
    val invalidNumericLineCounter = sc.longAccumulator("invalidNumericLineCounter")

    val filePath = s"file://${System.getProperty("user.dir")}/_data/Line_of_numbers.csv"
    val inFile = sc.textFile(filePath);

    val splitLines = inFile.flatMap(line => {
      try {
        val reader = new CSVReader(new StringReader(line))
        Some(reader.readNext())
      } catch {
        case _ => {
          invalidLineCounter.add(1)
          None
        }
      }
    })

    val numericData = splitLines.flatMap(line => {
      try {
        Some(line.map(_.toDouble))
      } catch {
        case _ => {
          invalidNumericLineCounter.add(1)
          None
        }
      }
    })

    val summedData = numericData.map(row => row.sum)

    println(summedData.collect().mkString(","))
    println(
      s"""Errors:
         |--> ${invalidLineCounter}
         |--> ${invalidNumericLineCounter}""".stripMargin)
  }
}
