package org.hsmak.sqlFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CrossTabOpsOnTitanic extends App {

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


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/titanic"


  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
    .appName("DatasetRunner")
    .getOrCreate()


  /** ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val titanicPassengersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/titanic3_02.csv")


  println("titanicPassengersDF has " + titanicPassengersDF.count() + " rows")
  titanicPassengersDF.show(5)
  titanicPassengersDF.printSchema()


  val passengersQuery = titanicPassengersDF.select(
    titanicPassengersDF("Pclass"),
    titanicPassengersDF("Survived"),
    titanicPassengersDF("Gender"),
    titanicPassengersDF("Age"),
    titanicPassengersDF("SibSp"),
    titanicPassengersDF("Parch"),
    titanicPassengersDF("Fare"))

  passengersQuery.show(5)
  passengersQuery.printSchema()


  passengersQuery.groupBy("Gender").count().show()
  passengersQuery.stat.crosstab("Survived", "Gender").show()
  //
  passengersQuery.stat.crosstab("Survived", "SibSp").show()
  //
  // passengers1.stat.crosstab("Survived","Age").show()
  val ageDist = passengersQuery.select(
    passengersQuery("Survived"),
    (passengersQuery("age") - passengersQuery("age") % 10).cast("int").as("AgeBracket"))

  ageDist.show(3)
  ageDist.stat.crosstab("Survived", "AgeBracket").show()
}
