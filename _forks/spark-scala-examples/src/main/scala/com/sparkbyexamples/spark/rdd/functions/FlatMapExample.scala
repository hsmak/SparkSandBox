package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object FlatMapExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  /*
   * Example 1
   */
  val data = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")
  val rdd = spark.sparkContext.parallelize(data)
  rdd.foreach(println)

  val rdd1 = rdd.flatMap(f => f.split(" "))
  rdd1.foreach(println)

  /*
   * Example 2
   */
  val arrayStructureData = Seq(
    Row("James,,Smith", List("Java", "Scala", "C++"), "CA"),
    Row("Michael,Rose,", List("Spark", "Java", "C++"), "NJ"),
    Row("Robert,,Williams", List("CSharp", "VB", "R"), "NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name", StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)


  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

  import spark.implicits._

  /*
   * Flattening the seq of languages of every person into separate records each
   * Notice how:
   *    - flatMap() is being used
   *    - Seq of Langs is being extracted
   *    - Row is being mapped into a Tuple3
   */
  val df2 = df.flatMap(row => row.getSeq[String](1).map((row.getString(0), _, row.getString(2))))
    .toDF("Name", "Language", "State")
  df2.show(false)

}
