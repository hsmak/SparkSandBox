package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDDFromCSVFile extends MyContext {

  def main(args:Array[String]): Unit ={

    def splitString(row:String):Array[String]={
      row.split(",")
    }

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile(s"$data_dir/zipcodes-noheader.csv")

    val rdd2:RDD[ZipCode] = rdd.map(row=>{
     val strArray = splitString(row)
      ZipCode(strArray(0).toInt,strArray(1),strArray(3),strArray(4))
    })

    rdd2.foreach(a=>println(a.city))
    println

    println("------------- loadIntoDataSet ----------------")
    loadCertainColsIntoDataSet(spark)
  }

  /**
    * Observations:
    *     - row.getInt(0) failed with ClassCastException!!! Not sure why??
    *     - Had to use row.getAs[String](0).toInt
    *
    * @param spark
    */
  def loadCertainColsIntoDataSet(spark:SparkSession) = {
    import spark.implicits._
    val ds = spark.read.csv(s"$data_dir/zipcodes-noheader.csv").map{ row =>
      ZipCode(row.getString(0).toInt, row.getString(1), row.getString(3), row.getString(4)) // ROW[ZipCode]
    }//.collect().foreach(a => println(a.city))
    ds.printSchema()
    ds.show
    ds.rdd.foreach(a => println(a.city))
    println

    //val schema = ScalaReflection.schemaFor[ZipCode].dataType.asInstanceOf[StructType]
  }

}


