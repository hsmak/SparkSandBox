package com.sparkbyexamples.spark.dataframe

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToAvroParquetJson extends App with MyContext {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read csv with options
  val df = spark
    .read
    .options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv(s"$data_dir/zipcodes.csv")
  df.show()
  df.printSchema()

  //convert to avro
  df
    .write
    .format("avro")
    .mode(SaveMode.Overwrite)
    .save(s"$out_dir/avro/zipcodes.avro")

  //convert to avro by partition
  df
    .write
    .partitionBy("State","Zipcode")
    .format("avro")
    .mode(SaveMode.Overwrite)
    .save(s"$out_dir/avro/zipcodes_partition.avro")

  //convert to parquet
  df
    .write
    .mode(SaveMode.Overwrite)
    .parquet(s"$out_dir/parquet/zipcodes.parquet")

  //convert to csv
  df
    .write
    .mode(SaveMode.Overwrite)
    .json(s"$out_dir/json/zipcodes.json")
}
