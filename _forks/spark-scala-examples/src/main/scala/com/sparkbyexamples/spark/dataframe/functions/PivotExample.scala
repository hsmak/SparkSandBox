package com.sparkbyexamples.spark.dataframe.functions

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * A pivot is an aggregation where one (or more in the general case) of the grouping columns has its distinct values transposed into individual columns.
  * Pivot tables are an essential part of data analysis and reporting.
  */
object PivotExample extends MyContext {
  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val data = Seq(
      ("Banana",1000,"USA"),
      ("Carrots",1500,"USA"),
      ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),
      ("Orange",2000,"USA"),
      ("Banana",400,"China"),
      ("Carrots",1200,"China"),
      ("Beans",1500,"China"),
      ("Orange",4000,"China"),
      ("Banana",2000,"Canada"),
      ("Carrots",2000,"Canada"),
      ("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    val groupedDF = df
      .groupBy("Product","Country")
      .sum("Amount")
    groupedDF.show

    //pivot
    val pivotDF = groupedDF
      .groupBy("Product")
      .pivot("Country") // must always follow groupBy().. Distinct Countries each will have their own column
      .sum("sum(Amount)") // Each Country will have the sum value
    pivotDF.show()

    val countries = Seq("USA","China","Canada","Mexico") // Manually, supplying the columns' names for the pivoting
    val pivotDF2 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF2.show()

    //unpivot
//    val unPivotDF = pivotDF.select($"Product",expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) " +
//      "as (Country,Total)")) //.where("Total is not null")
//    unPivotDF.show()

//    df.select(collect_list(""))

    }
}
