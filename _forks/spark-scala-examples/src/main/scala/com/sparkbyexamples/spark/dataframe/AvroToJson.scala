package com.sparkbyexamples.spark.dataframe

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object AvroToJson extends App with MyContext {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read avro file
  val df = spark.read.format("avro")
    .load(s"$data_dir/zipcodes.avro")
  df.show()
  df.printSchema()

  //convert to json
  df.write.mode(SaveMode.Overwrite)
    .json(s"$out_dir/json/zipcodes.json")

}
