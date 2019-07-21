package org.hsmak

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import vegas.DSL.SpecBuilder
import vegas._
import vegas.data.External._
import vegas.sparkExt._

import scala.io.Source._

/**
  * @author ${user.name}
  */
object VegasWrangling_Spark extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("VegasWrangling_Spark")
    .getOrCreate()

  import spark.implicits._

  def sc = spark.sparkContext




  val countryPopultionDF = spark.sparkContext.parallelize(Seq(
    ("country" -> "USA", "population" -> 314),
    ("country" -> "UK", "population" -> 64),
    ("country" -> "DK", "population" -> 80)
  ).map(t => (t._1._2, t._2._2)))
    .toDF("country", "population")


  val plot = Vegas("Country Pop").
    withDataFrame(countryPopultionDF)
    .encodeX("country", Nom)
    .encodeY("population", Quant)
    .mark(Bar)


  val simpleBarChartDF = spark.sparkContext.parallelize(Seq(
    ("a" -> "A", "b" -> 28), ("a" -> "B", "b" -> 55), ("a" -> "C", "b" -> 43),
    ("a" -> "D", "b" -> 91), ("a" -> "E", "b" -> 81), ("a" -> "F", "b" -> 53),
    ("a" -> "G", "b" -> 19), ("a" -> "H", "b" -> 87), ("a" -> "I", "b" -> 52)
  ).map(m => (m._1._2, m._2._2)))
    .toDF("a", "b")

  val SimpleBarChart =
    Vegas("A simple bar chart with embedded data.").
      withDataFrame(simpleBarChartDF)
      .encodeX("a", Ordinal)
      .encodeY("b", Quantitative)
      .mark(Bar)



  /**
    * Adding these from Master branch. They aren't in the package "vegas.data.External._" of the release yet.
    */
  val Github = "https://vega.github.io/vega-editor/app/data/github.csv"
  val Anscombe = "https://vega.github.io/vega-editor/app/data/anscombe.json"
  val SeattleWeather = "https://vega.github.io/vega-editor/app/data/seattle-weather.csv"


  val population_JsonRDD = sc.parallelize(fromURL(Population).mkString.stripLineEnd :: Nil)
  val population_DS = population_JsonRDD.toDS
  val population_DF = spark.read.option("header", true).json(population_DS)
  population_DF.show

  val cars_JsonRDD = sc.parallelize(fromURL(Cars).mkString.stripLineEnd :: Nil)
  val cars_DS = cars_JsonRDD.toDS
  val cars_DF = spark.read.option("header", true).json(cars_DS)
  cars_DF.show

  val unemployment_JsonRDD = sc.parallelize(fromURL(Unemployment).mkString.stripLineEnd :: Nil)
  val unemployment_DS = unemployment_JsonRDD.toDS
  val unemployment_DF = spark.read.option("header", true).json(unemployment_DS)
  unemployment_DF.show

  val movies_JsonRDD = sc.parallelize(fromURL(Movies).mkString.stripLineEnd :: Nil) // adding the whole json on one line
  val movies_DS = movies_JsonRDD.toDS
  val movies_DF = spark.read.option("header", true).json(movies_DS)
  movies_DF.show

  val barley_JsonRDD = sc.parallelize(fromURL(Barley).mkString.stripLineEnd :: Nil) // splitting on a new line
  val barley_DS = barley_JsonRDD.toDS
  val barley_DF = spark.read.option("header", true).json(barley_DS)
  barley_DF.show

  val stocks_CsvRDD = sc.parallelize(fromURL(Stocks).mkString.split("\n"))
  val stocks_DS = stocks_CsvRDD.toDS
  val stocks_DF = spark.read.option("header", true).csv(stocks_DS)
  stocks_DF.show

  val github_CsvRDD = sc.parallelize(fromURL(Github).mkString.split("\n"))
  val github_DS = github_CsvRDD.toDS
  val github_DF = spark.read.option("header", true).csv(github_DS)
  github_DF.show


  val anscombe_JsonRDD = sc.parallelize(fromURL(Anscombe).mkString.stripLineEnd :: Nil)
  val anscombe_DS = anscombe_JsonRDD.toDS
  val anscombe_DF = spark.read.option("header", true).json(anscombe_DS)
  anscombe_DF.show


  val seattleWeather_CsvRDD = sc.parallelize(fromURL(SeattleWeather).mkString.split("\n"))
  val seattleWeather_DS = seattleWeather_CsvRDD.toDS
  val seattleWeather_DF = spark.read.option("header", true).csv(seattleWeather_DS)
  seattleWeather_DF.show


  val AggregateBarChart =
    Vegas("A bar chart showing the US population distribution of age groups in 2000.").
      withDataFrame(population_DF).
      mark(Bar).
      filter("datum.year == 2000").
      encodeY("age", Ordinal, scale = Scale(bandSize = 17)).
      encodeX("people", Quantitative, aggregate = AggOps.Sum, axis = Axis(title = "population"))


  val GroupedBarChart =
    Vegas().
      withDataFrame(population_DF).
      mark(Bar).
      addTransformCalculation("gender", """datum.sex == 2 ? "Female" : "Male"""").
      filter("datum.year == 2000").
      encodeColumn("age", Ord, scale = Scale(padding = 4.0), axis = Axis(orient = Orient.Bottom, axisWidth = 1.0, offset = -8.0)).
      encodeY("people", Quantitative, aggregate = AggOps.Sum, axis = Axis(title = "population", grid = false)).
      encodeX("gender", Nominal, scale = Scale(bandSize = 6.0), hideAxis = true).
      encodeColor("gender", Nominal, scale = Scale(rangeNominals = List("#EA98D2", "#659CCA"))).
      configFacet(cell = CellConfig(strokeWidth = 0))


  val AreaChart =
    Vegas().
      withDataFrame(unemployment_DF).
      mark(Area).
      encodeX("date", Temp, timeUnit = TimeUnit.Yearmonth, scale = Scale(nice = Nice.Month),
        axis = Axis(axisWidth = 0, format = "%Y", labelAngle = 0)).
      encodeY("count", Quantitative, aggregate = AggOps.Sum).
      configCell(width = 300, height = 200)


  val NormalizedStackedBarChart =
    Vegas().
      withDataFrame(population_DF).
      filter("datum.year == 2000").
      addTransform("gender", "datum.sex == 2 ? \"Female\" : \"Male\"").
      mark(Bar).
      encodeY("people", Quant, AggOps.Sum, axis = Axis(title = "population")).
      encodeX("age", Ord, scale = Scale(bandSize = 17)).
      encodeColor("gender", Nominal, scale = Scale(rangeNominals = List("#EA98D2", "#659CCA"))).
      configMark(stacked = StackOffset.Normalize)


  val BinnedChart =
    Vegas("A trellis scatterplot showing Horsepower and Miles per gallons, faceted by binned values of Acceleration.").
      withDataFrame(cars_DF).
      mark(Point).
      encodeX("Horsepower", Quantitative).
      encodeY("Miles_per_Gallon", Quantitative).
      encodeRow("Acceleration", Quantitative, enableBin = true)


  val ScatterBinnedPlot =
    Vegas().
      withDataFrame(movies_DF).
      mark(Point).
      encodeX("IMDB_Rating", Quantitative, bin = Bin(maxbins = 10.0)).
      encodeY("Rotten_Tomatoes_Rating", Quantitative, bin = Bin(maxbins = 10.0)).
      encodeSize(aggregate = AggOps.Count, field = "*", dataType = Quantitative)


  val ScatterColorPlot =
    Vegas().
      withDataFrame(cars_DF).
      mark(Point).
      encodeX("Horsepower", Quantitative).
      encodeY("Miles_per_Gallon", Quantitative).
      encodeColor(field = "Origin", dataType = Nominal)

  val ScatterBinnedColorPlot =
    Vegas("A scatterplot showing horsepower and miles per gallons with binned acceleration on color.").
      withDataFrame(cars_DF).
      mark(Point).
      encodeX("Horsepower", Quantitative).
      encodeY("Miles_per_Gallon", Quantitative).
      encodeColor(field = "Acceleration", dataType = Quantitative, bin = Bin(maxbins = 5.0))

  val StackedAreaBinnedPlot =
    Vegas().
      withDataFrame(cars_DF).
      mark(Area).
      encodeX("Acceleration", Quantitative, bin = Bin()).
      encodeY("Horsepower", Quantitative, AggOps.Mean, enableBin = false).
      encodeColor(field = "Cylinders", dataType = Nominal)

  val SortColorPlot =
    Vegas("The Trellis display by Becker et al. helped establish small multiples as a “powerful mechanism for understanding interactions in studies of how a response depends on explanatory variables”. Here we reproduce a trellis of Barley yields from the 1930s, complete with main-effects ordering to facilitate comparison.").
      withDataFrame(barley_DF).
      mark(Point).
      encodeRow("site", Ordinal).
      encodeX("yield", Quantitative, aggregate = AggOps.Mean).
      encodeY("variety", Ordinal, sortField = Sort("yield", AggOps.Mean), scale = Scale(bandSize = 12.0)).
      encodeColor(field = "year", dataType = Nominal)

  val CustomShapePlot =
    Vegas("A scatterplot with custom star shapes.").
      withDataFrame(cars_DF).
      mark(Point).
      encodeX("Horsepower", Quant).
      encodeY("Miles_per_Gallon", Quant).
      encodeColor("Cylinders", Nom).
      encodeSize("Weight_in_lbs", Quant).
      configMark(customShape = "M0,0.2L0.2351,0.3236 0.1902,0.0618 0.3804,-0.1236 0.1175,-0.1618 0,-0.4 -0.1175,-0.1618 -0.3804,-0.1236 -0.1902,0.0618 -0.2351,0.3236 0,0.2Z")

  val ScatterAggregateDetail =
    Vegas("A scatterplot showing average horsepower and displacement for cars from different origins.").
      withDataFrame(cars_DF).
      mark(Point).
      encodeX("Horsepower", Quant, AggOps.Mean).
      encodeY("Displacement", Quant, AggOps.Mean).
      encodeDetail("Origin")

  val LineDetail =
    Vegas("Stock prices of 5 Tech Companies Over Time.").
      withDataFrame(stocks_DF).
      mark(Line).
      encodeX("date", Temp).
      encodeY("price", Quant).
      encodeDetailFields(Field(field = "symbol", dataType = Nominal))


  val GithubPunchCard =
    Vegas().
      withDataFrame(github_DF).
      mark(Circle).
      encodeX("time", Temporal, timeUnit = TimeUnit.Hours).
      encodeY("time", Temporal, timeUnit = TimeUnit.Day).
      encodeSize("count", Quantitative, aggregate = AggOps.Sum)

  val AnscombesQuartet =
    Vegas("Anscombe's Quartet").
      withDataFrame(anscombe_DF).
      mark(Circle).
      encodeX("X", Quantitative, scale = Scale(zero = false)).
      encodeY("Y", Quantitative, scale = Scale(zero = false)).
      encodeColumn("Series", Nominal).
      configMark(opacity = 1)

  val StackedAreaChart =
    Vegas("Area chart showing weight of cars over time.").
      withDataFrame(unemployment_DF).
      mark(Area).
      encodeX(
        "date", Temporal, timeUnit = TimeUnit.Yearmonth,
        axis = Axis(axisWidth = 0, format = "%Y", labelAngle = 0),
        scale = Scale(nice = spec.Spec.NiceTimeEnums.Month)
      ).
      encodeY("count", Quantitative, aggregate = AggOps.Sum).
      encodeColor("series", Nominal, scale = Scale(rangePreset = Category20b)).
      configCell(width = 300, height = 200)

  val NormalizedStackedAreaChart =
    Vegas().
      withDataFrame(unemployment_DF).
      mark(Area).
      encodeX(
        "date", Temporal, timeUnit = TimeUnit.Yearmonth,
        axis = Axis(axisWidth = 0, format = "%Y", labelAngle = 0),
        scale = Scale(nice = spec.Spec.NiceTimeEnums.Month)
      ).
      encodeY("count", Quantitative, aggregate = AggOps.Sum, hideAxis = Some(true)).
      encodeColor("series", Nominal, scale = Scale(rangePreset = Category20b)).
      configCell(width = 300, height = 200).
      configMark(stacked = StackOffset.Normalize)

  val Streamgraph =
    Vegas().
      withDataFrame(unemployment_DF).
      mark(Area).
      encodeX(
        "date", Temporal, timeUnit = TimeUnit.Yearmonth,
        axis = Axis(axisWidth = 0, format = "%Y", labelAngle = 0, tickSize = Some(0.0)),
        scale = Scale(nice = spec.Spec.NiceTimeEnums.Month)
      ).
      encodeY("count", Quantitative, aggregate = AggOps.Sum, hideAxis = Some(true)).
      encodeColor("series", Nominal, scale = Scale(rangePreset = Category20b)).
      configCell(width = 300, height = 200).
      configMark(stacked = StackOffset.Center)

  val StackedBarChart =
    Vegas().
      withDataFrame(seattleWeather_DF).
      mark(Bar).
      encodeX("date", Temporal, timeUnit = TimeUnit.Month, axis = Axis(title = "Month of the year")).
      encodeY("*", Quantitative, aggregate = AggOps.Count).
      encodeColor("weather", Nominal, scale = Scale(
        domainNominals = List("sun", "fog", "drizzle", "rain", "snow"),
        rangeNominals = List("#e7ba52", "#c7c7c7", "#aec7e8", "#1f77b4", "#9467bd")),
        legend = Legend(title = "Weather type"))

  val StripPlot =
    Vegas("Shows the relationship between horsepower and the numbver of cylinders using tick marks.").
      withDataFrame(cars_DF).
      mark(Tick).
      encodeX("Horsepower", Quantitative).
      encodeY("Cylinders", Ordinal)

  // Names (ex. bar, bar_aggregate, etc.) are corresponding to filenames
  //  of `/core/src/test/resources/example-specs/*.vl.json`
  val plotsWithNames: List[(String, SpecBuilder)] = List(
    "bar" -> SimpleBarChart,
    "bar_aggregate" -> AggregateBarChart,
    "bar_grouped" -> GroupedBarChart,
    "area" -> AreaChart,
    "stacked_bar_normalize" -> NormalizedStackedBarChart,
    "scatter_binned" -> ScatterBinnedPlot,
    "scatter_color" -> ScatterColorPlot,
    "scatter_binned_color" -> ScatterBinnedColorPlot,
    "stacked_area_binned" -> StackedAreaBinnedPlot,
    "trellis_barley" -> SortColorPlot,
    "trellis_scatter_binned_row" -> BinnedChart,
    "scatter_shape_custom" -> CustomShapePlot,
    "line_detail" -> LineDetail,
    "github_punchcard" -> GithubPunchCard,
    "trellis_anscombe" -> AnscombesQuartet,
    "stacked_area" -> StackedAreaChart,
    "stacked_area_normalize" -> NormalizedStackedAreaChart,
    "stacked_area_stream" -> Streamgraph,
    "stacked_bar_weather" -> StackedBarChart,
    "tick_strip" -> StripPlot
  ).sortBy(_._1)

  val plots: List[SpecBuilder] = plotsWithNames.map(_._2)

  plots.foreach(_.show)


}
