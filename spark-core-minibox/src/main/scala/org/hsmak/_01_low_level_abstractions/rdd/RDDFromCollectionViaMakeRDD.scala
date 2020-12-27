package org.hsmak._01_low_level_abstractions.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RDDFromCollectionViaMakeRDD extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark: SparkSession = SparkSession
    .builder()
    .appName("RDDFromCollections")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  // total of 10 rows
  val  valToBeSquared = 1 to 5
  val valToBeCubed = 6 to 10

  val squaresRDD = sc.makeRDD(valToBeSquared).map(i => (i, i * i, "square")) // it calls parallelize() under the hood
  val cubesRDD = sc.makeRDD(valToBeCubed).map(i => (i, i * i * i, "cube")) // it calls parallelize() under the hood

  // Below is all about DataFrames
  val squaresDF = squaresRDD.toDF("value", "result", "op")
  val cubesDF = cubesRDD.toDF("value", "result", "op")

  // Same thing can be achieved via sc.parallelize()
//  val squaresDF = sc.parallelize(valToBeSquared).map(i => (i, i * i, "square")).toDF("value", "result", "op")
//  val cubesDF = sc.parallelize(valToBeCubed).map(i => (i, i * i * i, "cube")).toDF("value", "result", "op")

  squaresDF.unionByName(cubesDF).show()
}