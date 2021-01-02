package com.sparkbyexamples.spark.dataframe.examples

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object RepartitionExample extends App with MyContext {

  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
//    .config("spark.default.parallelism", "500")
    .getOrCreate()

 // spark.sqlContext.setConf("spark.default.parallelism", "500")
  //spark.conf.set("spark.default.parallelism", "500")
  val df = spark.range(0,20)
  df.printSchema()
  println(df.rdd.partitions.length)

  df.write.mode(SaveMode.Overwrite).csv(s"$out_dir/repart/df-partition.csv")

  val df2 = df.repartition(10)

  println(df2.rdd.partitions.length)

 val df3 = df.coalesce(2)
 println(df3.rdd.partitions.length)

 val df4 = df.groupBy("id").count()
 println(df4.rdd.getNumPartitions)
}
