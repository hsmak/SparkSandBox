package com.sparkbyexamples.spark.dataframe

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveDataFrame extends MyContext{

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val filePath = s"$data_dir/zipcodes.csv"

    var df:DataFrame = spark.read.option("header","true").csv(filePath)

    df.repartition(5).write.option("header","true").csv(s"$out_dir/parquet/df1")
  }
}
