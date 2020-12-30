package com.sparkbyexamples.spark.dataframe

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession

object FromCSVFile2 extends MyContext{

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val filePath=s"$data_dir/stream.csv"

    val df3 = spark.read.option("header",true).csv(s"$data_dir/zipcodes.csv")
    df3.show(false)


    val df = spark.read.options(Map("inferSchema"->"true","delimiter"->"|","header"->"true")).csv(filePath)

    val df2 = df.select("Gender", "BirthDate", "TotalCost", "TotalChildren", "ProductCategoryName")
      .filter("Gender is not null")
      .filter("BirthDate is not null")
      .filter("TotalChildren is not null")
      .filter("ProductCategoryName is not null")
    df2.show()

    df.select("Gender", "BirthDate", "TotalCost", "TotalChildren", "ProductCategoryName")
      .where(df("Gender").isNotNull && df("BirthDate").isNotNull && df("TotalChildren").isNotNull && df("ProductCategoryName").isNotNull ).show()

  }
}
