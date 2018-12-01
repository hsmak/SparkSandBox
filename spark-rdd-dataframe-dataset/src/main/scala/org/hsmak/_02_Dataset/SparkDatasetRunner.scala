package org.hsmak._02_Dataset

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object SparkDatasetRunner {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SparkDatasetRunner")
      .getOrCreate()

    import spark.implicits._

    val filePath = "file:///home/hsmak/Development/git/spark-sandbox/spark-rdd-dataframe-dataset/src/main/resources/war-and-peace.txt"
    // via SparkSQL
    val dataset = spark.read.textFile(filePath)
      .flatMap(line => line.split(" "))
      .map(w => (w, 1))//why can't reduceByKey on Dataset???

    dataset.show()

    val df = dataset.toDF("words", "frequency")
    df.show()


    spark.stop()
  }

}
