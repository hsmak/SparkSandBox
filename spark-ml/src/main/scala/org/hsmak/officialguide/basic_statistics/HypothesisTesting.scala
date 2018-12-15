package org.hsmak.officialguide.basic_statistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Hypothesis testing is a powerful tool in statistics to determine whether a result is statistically significant,
  * whether this result occurred by chance or not.
  *
  * spark.ml currently supports Pearson’s Chi-squared ( χ2) tests for independence.
  */
object HypothesisTesting extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("HypothesisTesting")
    .getOrCreate()


  val data = Seq(
    (0.0, Vectors.dense(0.5, 10.0)),
    (0.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (0.0, Vectors.dense(3.5, 30.0)),
    (0.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
  )

  import spark.implicits._

  val df = data.toDF("label", "features")
  df.show

  val Row(coeff: Matrix) = Correlation.corr(df, "features").head
  println(s"correlation matrix: \n${coeff}")
  println

  val chi = ChiSquareTest.test(df, "features", "label").head

  println(s"pValues = ${chi.getAs[Vector](0)}")
  println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
  println(s"statistics ${chi.getAs[Vector](2)}")

}
