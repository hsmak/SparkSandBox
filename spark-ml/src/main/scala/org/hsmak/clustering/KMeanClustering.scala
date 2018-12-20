package org.hsmak.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

object KMeanClustering extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"


  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("KMeanClustering")
    .getOrCreate()


  // Loads data.
  val dataset = spark.read.format("libsvm")
    .load(s"${base_data_dir}/sample_kmeans_data.txt")

  // Trains a k-means model.
  val kmeans = new KMeans()
    .setK(2)
    .setSeed(1L)
  val model = kmeans.fit(dataset)

  // Make predictions
  val predictions = model.transform(dataset)
  predictions.show

  // Evaluate clustering by computing Silhouette score
  val evaluator = new ClusteringEvaluator()
  val silhouette = evaluator.evaluate(predictions)
  println(s"Silhouette with squared euclidean distance = $silhouette")

  // Shows the result.
  println("Cluster Centers: ")
  model.clusterCenters.foreach(println)

}
