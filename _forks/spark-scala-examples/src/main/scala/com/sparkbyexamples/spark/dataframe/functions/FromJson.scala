package com.sparkbyexamples.spark.dataframe.functions

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

object FromJson extends MyContext{
  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()


    val data = Seq(
      ("1","{\"name\":\"Anne\",\"Age\":\"12\",\"country\":\"Denmark\"}"),
      ("2","{\"name\":\"Zen\",\"Age\":\"24\"}"),
      ("3","{\"name\":\"Fred\",\"Age\":\"20\",\"country\":\"France\"}"),
      ("4","{\"name\":\"Mona\",\"Age\":\"18\",\"country\":\"Denmark\"}"))

    import spark.sqlContext.implicits._
    val df = data.toDF("ID","details_Json")

    val jsonSchema = new StructType()
      .add("name",StringType,true)
      .add("Age",StringType,true)
      .add("country",StringType,true)

    val df2 = df.withColumn("details_Struct", from_json($"details_Json", jsonSchema))
      .withColumn("name",col("details_Struct.name")) // Using the "." notation
      .withColumn("age",col("details_Struct.age")) // Using the "." notation
      .withColumn("country",col("details_Struct").getField("country")) // Using getField() method
      .filter(col("country").equalTo("Denmark"))

    df2.printSchema()
    df2.show(false)

    // Alternatively, we can flatten out the JsonStruct in one show
    df.withColumn("details_Struct", from_json($"details_Json", jsonSchema))
      .select($"*", $"details_Struct.*") // Using the "." notation
      .filter(col("country").equalTo("Denmark"))
      .show(false)
  }
}
