package org.hsmak.regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorSlicer}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LinearRegressionOnHousePrices extends App {

  //turn off Logging
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("House Prices'")
    .getOrCreate()


  val df = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("file:///home/hsmak/Development/git/spark-sandbox/_data/house_prices/train.csv")

  import spark.implicits._

  df.createOrReplaceTempView("houses")

  //Store the target column in a val <- the column whose value to be predicted
  val labelField = "SalePrice"

  /**
    * SparkMLlib - Machine Learning: Feature Transformation
    */

  /**
    * Categorical features transformation - Transform from String to Numerical/Scalalr
    */

  // Extract all columns except "Id" and the to-be-predicted one "SalePrice"
  /* val scalarFields: Seq[String] = df.schema.fields.collect {
     case StructField(name, IntegerType, _, _) if name != labelField && name != "Id" => name
   }

   val scalarData = df.map { row =>
     //Tuple2
     (Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray),
       row.getInt(row.fieldIndex(labelField)))

   }.toDF("features", "labels")*/

  val scalarFields: Seq[String] = df.schema.fields.collect {
    case StructField(name, IntegerType, _, _) if name != labelField && name != "Id" => name
  }
  val scalarData = df.map { row =>
    //Tuple2
    (
      Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray),
      row.getInt(row.fieldIndex(labelField))
    )
  }.toDF("scalar_features", "labels")

  scalarData.show


  // this time we're retreiving StringTyped columns
  val stringFields: Seq[String] = df.schema.fields.collect {

    case StructField(name, StringType, _, _) => name

  }

  /**
    * Create a unique value/index for the categorial values such as 1StoryBuilding, 2StoryBuilding, etc
    */
  val dfWithCategories = stringFields.foldLeft(df) { (dfTemp, nxtCol) =>
    val indexer = new StringIndexer()
      .setInputCol(nxtCol)
      .setOutputCol(s"${nxtCol}_id")

    indexer.fit(dfTemp).transform(dfTemp)
  }

  //Test with "HouseStyle"
  val resultsAfterTrans = dfWithCategories.select("HouseStyle", "HouseStyle_id").distinct()
  resultsAfterTrans.show


  val data = dfWithCategories.map { row =>
    //Tuple3
    (
      //ScalaFields
      Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray),
      //StringFields which was transformed into Scalar ones
      Vectors.dense(stringFields.map(name => row.getAs[Double](s"${name}_id").toDouble).toArray),
      row.getInt(row.fieldIndex(labelField))

    )

  }.toDF("features_scalar", "features_categorical", "label") // Renaming _.1, _.2, _.3, respectively

  data.show

  /**
    * Scaling Scalar Feature <- Normalization??
    * Via Standard Deviation & Mean <- Review the material from @ StanfordUnversity
    */

  val stdScaler = new StandardScaler()
    .setInputCol("features_scalar")
    .setOutputCol("features_scalar_scaled")
    .setWithStd(true)
    .setWithMean(true)

  val data_scaled = stdScaler.fit(data).transform(data)

  /**
    * Combining Data: Categorical + Scalar
    */

  val dataCombined = data_scaled.map { row =>

    val combinedFeatures = Vectors.dense(row.getAs[DenseVector]("features_scalar_scaled").values ++
      row.getAs[DenseVector]("features_categorical").values)

    (combinedFeatures, row.getAs[Int]("label")) //Returning Tuple2

  }.toDF("features", "label")
  dataCombined.show

  /**
    * Feature Selectors
    */


  /* val scalarFields: Seq[String] = df.schema.fields.collect{
     case StructField(name, IntegerType, _, _) if name != labelField => name
   }
   val scalarData = df.map{ row =>
     //Tuple2
     (
       Vectors.dense(scalarFields.map(name => row.getAs[Int](name).toDouble).toArray),
       row.getInt(row.fieldIndex(labelField))
     )
   }.toDF("scalar_features", "labels")*/

  /**
    * Notice the "(1 until scalarFields.size)" is skipping the first index which the ID in the features table
    */
  val slicer = new VectorSlicer()
    .setInputCol("scalar_features")
    .setOutputCol("features")
    .setIndices((1 until scalarFields.size).toArray)

  val output = slicer.transform(scalarData)

  output.show


  /**
    * Modeling and Evaluation - Common Logic
    */

  // extract trainData & testData using Extractors
  val Array(trainData, testData) = dataCombined.randomSplit(Array(0.7, 0.3))

  def evaluate(ds: DataFrame, model: Transformer): (DataFrame, Double) = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    val predictions = model.transform(ds)
    val r2 = evaluator.evaluate(predictions)

    (predictions, r2)

  }

  /**
    * Linear Regression
    */

  // Instantiate the LinearRegression instance
  val lr = new LinearRegression()
    .setMaxIter(1000)
    .setRegParam(0.3)

  // Fit the Model
  val lrModel = lr.fit(trainData)// ML Training and Fitting happens here!

  // Evaluation
  val (trainLrPredictions, trainLrR2) = evaluate(trainData, lrModel)
  val (testLrPredictions, testLrR2) = evaluate(testData, lrModel)

  // Output
  println(s"r2 on train data: $trainLrR2")
  println(s"r2 on test data: $testLrR2")

  val tableOutput = testLrPredictions.select($"prediction", $"label").show(10)

  // z.show(tableOutput)
}
