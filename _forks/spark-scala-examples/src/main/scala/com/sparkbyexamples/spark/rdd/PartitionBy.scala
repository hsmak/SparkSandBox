package com.sparkbyexamples.spark.rdd

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object PartitionBy {

  val base_dir = s"file://${System.getProperty("user.dir")}"
  val data_dir = base_dir + "/_forks/spark-scala-examples/src/main/resources"

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    saveWithPartitionsFromRDD(spark)
    saveWithPartitionsFromDF(spark)
  }

  /**
    * Saving an RDD allows to partition by some concrete Partitioners
    *   - Less control and flexibility
    *
    * @param spark
    */
  def saveWithPartitionsFromRDD(spark: SparkSession) = {
    val sc = spark.sparkContext

    val rdd = sc.textFile(s"$data_dir/zipcodes.csv")

    val rdd2: RDD[Array[String]] = rdd.map(m => m.split(","))


    val rdd3 = rdd2.map(a => (a(1), a.mkString(",")))

    val rdd4 = rdd3.partitionBy(new HashPartitioner(3))

    // Can't overwrite exiting partitions!?!?
    rdd4.saveAsTextFile(s"$base_dir/_data/OUT/partitionsFrmRDD")
  }

  /**
    * Saving a DataFrame allows to partition by "column(s)"
    *   - More control and flexibility
    *
    * @param spark
    */
  def saveWithPartitionsFromDF(spark: SparkSession) = {
    //    val sc = spark.sparkContext;
    val df = spark.read.option("inferSchema", true).option("header", true).csv(s"$data_dir/zipcodes.csv")
    df.show(3)
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .partitionBy("State")
      .csv(s"$base_dir/_data/OUT/partitionsFromDF")
  }


}
