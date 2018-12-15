package org.hsmak.officialguide.feature.transformers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object StringIndexerTransformer extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StringIndexerTransformer")
    .getOrCreate()

  val df = spark.createDataFrame(
    Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
  ).toDF("id", "category")

  val indexer = new StringIndexer()
    .setInputCol("category")
    .setOutputCol("categoryIndex")

  val indexed = indexer.fit(df).transform(df)
  indexed.show()

}
