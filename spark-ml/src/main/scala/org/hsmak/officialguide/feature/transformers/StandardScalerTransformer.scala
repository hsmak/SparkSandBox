package org.hsmak.officialguide.feature.transformers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession

object StandardScalerTransformer extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StandardScalerTransformer")
    .getOrCreate()

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"

  val dataFrame = spark.read
    .format("libsvm")
    .load(s"${base_data_dir}/sample_libsvm_data.txt")

  dataFrame.show

  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(false)

  // Compute summary statistics by fitting the StandardScaler.
  val scalerModel = scaler.fit(dataFrame)

  // Normalize each feature to have unit standard deviation.
  val dfWithScaledFeatures = scalerModel.transform(dataFrame)
  dfWithScaledFeatures.show()

}
