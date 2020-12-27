package org.hsmak._02_high_level_abstractions.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object DataFrameFromTextFile {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("DatasetRunner")
      .getOrCreate()

    import spark.implicits._

    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"

    // Notice the use of SparkSession not SparkContext to emit DF/DS not RDD
    val stringDS = spark.read.textFile(filePath)

    val dataset = stringDS // Transformation ops will emit Dataset
      .flatMap(line => line.split(",")) //spark.read.csv() will take care of all this
      .map(w => (w, 1))//why can't reduceByKey on Dataset???

    dataset.show()

    val df2 = dataset.toDF("words", "frequency")
    df2.show()

    spark.stop()
  }
}
