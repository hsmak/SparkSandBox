package org.hsmak.officialguide.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object KMeanClusteringOnXYPlot extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"


  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("KMeanClusteringOnXYPlot")
    .getOrCreate()


  /** ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val xyPlotDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/cluster-points-v2.csv")


  println("carMileageDF has " + xyPlotDF.count() + " rows")
  xyPlotDF.show(5)
  xyPlotDF.printSchema()

  val assembler = new VectorAssembler()
    .setInputCols(Array("X", "Y"))
    .setOutputCol("features")

  val xyPlotWithFeatures = assembler.transform(xyPlotDF)
  xyPlotWithFeatures.show

  // Create the Kmeans model

  var algKMeans = new KMeans()
    .setK(2)
  var mdlKMeans = algKMeans.fit(xyPlotWithFeatures)

  // Print the summary.
  println("Cluster Centers (K=2) : " + mdlKMeans.clusterCenters.mkString("<", ",", ">"))
  println("Cluster Sizes (K=2) : " + mdlKMeans.summary.clusterSizes.mkString("<", ",", ">"))


  // Deprecated
  // Evaluate clustering by computing Within Set Sum of Squared Errors.
  //  var WSSSE = mdlKMeans.computeCost(xyPlotWithFeatures)
  //  println(s"Within Set Sum of Squared Errors (K=2) = %.3f".format(WSSSE))
  ////////////////////////////////


  val predictions = mdlKMeans.transform(xyPlotWithFeatures)
  predictions.show

  val evaluator = new ClusteringEvaluator()
  val silhouette = evaluator.evaluate(predictions)
  println(s"\n\n\nSilhouette with squared euclidean distance = $silhouette")

  // Shows the result.
  println("Cluster Centers: ")
  mdlKMeans.clusterCenters.foreach(println)

}
