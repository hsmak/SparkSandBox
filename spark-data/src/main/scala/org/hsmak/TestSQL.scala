package org.hsmak

import org.apache.spark.sql.SparkSession


object TestSQL extends App {

  case class Employee(EmployeeID: String,
                      LastName: String, FirstName: String, Title: String,
                      BirthDate: String, HireDate: String,
                      City: String, State: String, Zip: String, Country: String,
                      ReportsTo: String)

  case class Order(OrderID: String, CustomerID: String, EmployeeID: String,
                   OrderDate: String, ShipCountry: String)

  case class OrderDetails(OrderID: String, ProductID: String, UnitPrice: Double,
                          Qty: Int, Discount: Double)


  val spark = SparkSession
    .builder
    .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
    .appName("DatasetRunner")
    .getOrCreate()

  import spark.implicits._
  //  import org.apache.spark.sql._

  val filePath = s"file://${System.getProperty("user.dir")}/_data/fdps-v3-master/data/NW/NW-Employees.csv"

  val employees = spark.read
    .option("header", "true")
    .csv(filePath)
    .as[Employee]// binding to a type

  println("Employees has " + employees.count() + " rows")
  employees.show(5)
  employees.head()

}
