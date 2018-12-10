package org.hsmak.sqlFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ScientificOpsOnHypot extends App {

  //turn off Logging
  Logger.getLogger("org").setLevel(Level.OFF)


  case class Employee(EmployeeID: String,
                      LastName: String, FirstName: String, Title: String,
                      BirthDate: String, HireDate: String,
                      City: String, State: String, Zip: String, Country: String,
                      ReportsTo: String)

  case class Order(OrderID: String, CustomerID: String, EmployeeID: String,
                   OrderDate: String, ShipCountry: String)

  case class OrderDetails(OrderID: String, ProductID: String, UnitPrice: Double,
                          Qty: Int, Discount: Double)

  case class Product(ProductID: String, ProductName: String, UnitPrice: Double, UnitsInStock: Int, UnitsOnOrder: Int, ReorderLevel: Int, Discontinued: Int)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data"


  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
    .appName("DatasetRunner")
    .getOrCreate()


  val aList: List[Int] = List(10, 100, 1000)
  var aRDD = spark.sparkContext.parallelize(aList)
  val sqlContext = spark.sqlContext

  import sqlContext.implicits._

  val ds = spark.createDataset(aRDD)
  ds.show()

  import org.apache.spark.sql.functions.{log, log10, sqrt}

  ds.select(ds("value"), log(ds("value")).as("ln")).show()
  ds.select(ds("value"), log10(ds("value")).as("log10")).show()
  ds.select(ds("value"), sqrt(ds("value")).as("sqrt")).show()

  /** ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val hypotDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/hypot.csv")


  println("Data has " + hypotDF.count() + " rows")
  hypotDF.show(5)
  hypotDF.printSchema()

  import org.apache.spark.sql.functions.{hypot}
  hypotDF.select(hypotDF("X"), hypotDF("Y"), hypot(hypotDF("X"), hypotDF("Y")).as("hypot")).show()
}
