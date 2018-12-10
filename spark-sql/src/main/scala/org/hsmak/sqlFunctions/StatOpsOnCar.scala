package org.hsmak.sqlFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StatOpsOnCar extends App {

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


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/car-data"


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


  val carMileageDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/car-mileage.csv")


  println("carMileageDF has " + carMileageDF.count() + " rows")
  carMileageDF.show(5)
  carMileageDF.printSchema()

  val cor = carMileageDF.stat.corr("hp","weight")
  println("hp to weight : Correlation = %.4f".format(cor))
  
  val cov = carMileageDF.stat.cov("hp","weight")
  println("hp to weight : Covariance = %.4f".format(cov))

  //TODO - Understand how the `CrossTabulation` function provides a table of the frequency distribution for a set of variables!
  carMileageDF.stat.crosstab("automatic","NoOfSpeed").show()
}
