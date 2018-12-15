package org.hsmak.officialguide.basic_statistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Correlation computes the correlation matrix for the input Dataset of Vectors using the specified method.
  * The output will be a DataFrame that contains the correlation matrix of the column of vectors.
  */

/**
  * Link:
  *   - https://www.displayr.com/what-is-a-correlation-matrix/correlation matrix for the input Dataset of Vectors; not two columns!
  *
  * A correlation matrix is a table showing correlation coefficients between variables.
  * Each cell in the table shows the correlation between two variables.
  * A correlation matrix is used as a way to summarize data, as an input into a more advanced analysis, and as a diagnostic for advanced analyses.
  *
  */
object CorrelationMatrix extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("CorrelationMatrix")
    .getOrCreate()


  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
  )

  import spark.implicits._

  val df = data.map(Tuple1.apply).toDF("features")
  df.show


  // TODO - this is different from correlation between two columns?

  /**
    * From the definition: Compute the Pearson correlation matrix for the input Dataset of Vectors; not two columns!
    * Note the diagonal is all the way 1's:
    * |1 b c|
    * |a 1 c|
    * |a b 1|
    */


  // Extract the correlation matrix using *Pearson* method (default)
  val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
  println(s"Pearson correlation matrix:\n $coeff1")

  println

  // Extract the correlation matrix using *Spearman* method
  val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
  println(s"Spearman correlation matrix:\n $coeff2")

}
