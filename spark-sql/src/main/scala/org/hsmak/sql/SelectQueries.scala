package org.hsmak.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SelectQueries extends App {

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


  val spark = SparkSession
    .builder
    .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
    .appName("DatasetRunner")
    .getOrCreate()


  import spark.implicits._

  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/fdps-v3-master"


  /** ******************************************************
    * ###################### Loading CSVs ##################
    * ******************************************************/


  val employees = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/data/NW/NW-Employees.csv")
    .as[Employee] // binding to a type

  println("Employees has " + employees.count() + " rows")
  employees.show(5)
  println(employees.head())
  employees.dtypes.foreach(println) // verify column types


  val orders = spark.read.option("header", "true").
    csv(s"$base_data_dir/data/NW/NW-Orders.csv").as[Order]
  println("Orders has " + orders.count() + " rows")
  orders.show(5)
  println(orders.head())
  orders.dtypes.foreach(println)


  val orderDetails = spark.read.option("header", "true").
    option("inferSchema", "true").
    csv(s"$base_data_dir/data/NW-Order-Details.csv").as[OrderDetails]
  println("Order Details has " + orderDetails.count() + " rows")
  orderDetails.show(5)
  println(orderDetails.head())
  orderDetails.dtypes.foreach(println) // verify column types


  /** ******************************************************
    * ###################### Spark SQL #####################
    * *******************************************************/

  ///// Creating Views/Tables

  employees.createOrReplaceTempView("EmployeesTable")
  orders.createOrReplaceTempView("OrdersTable")
  orderDetails.createOrReplaceTempView("OrderDetailsTable")


  ////// SQL SELECT Statements

  //Employee

  val employeeSelect = spark.sql("SELECT * from EmployeesTable")
  employeeSelect.show(5)
  employeeSelect.head(3).foreach(println)
  employees.explain(true)

  val employeeSelectWhere = spark.sql("SELECT * from EmployeesTable WHERE State = 'WA'")
  employeeSelectWhere.show(5)
  employeeSelectWhere.head(3).foreach(println)
  employeeSelectWhere.explain(true)


  //Orders

  val orderSelect = spark.sql("SELECT * from OrdersTable")
  orderSelect.show(10)
  orderSelect.head(3).foreach(println)


  //OrderDetails

  val orderDetailSelect = spark.sql("SELECT * from OrderDetailsTable")
  orderDetailSelect.show(10)
  orderDetailSelect.head(3).foreach(println)



  /** ******************************************************
    * ################# Joining Tables #####################
    * *******************************************************/



  val Orders_JOIN_OrderDetails = spark.sql(
    """SELECT OrderDetailsTable.OrderID, ShipCountry, UnitPrice, Qty, Discount
      |FROM OrdersTable INNER JOIN OrderDetailsTable
      |ON OrdersTable.OrderID = OrderDetailsTable.OrderID""".stripMargin)
  Orders_JOIN_OrderDetails.show(10)
  Orders_JOIN_OrderDetails.head(3).foreach(println)


  // Sales By Country

  val Orders_JOIN_OrderDetails_GROUPBY_ShipCountry = spark.sql(
    """SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales
      |FROM OrdersTable INNER JOIN OrderDetailsTable
      |ON OrdersTable.OrderID = OrderDetailsTable.OrderID
      |GROUP BY ShipCountry""".stripMargin)
  println(Orders_JOIN_OrderDetails_GROUPBY_ShipCountry.count())
  Orders_JOIN_OrderDetails_GROUPBY_ShipCountry.show(10)
  Orders_JOIN_OrderDetails_GROUPBY_ShipCountry.head(3).foreach(println)

  Orders_JOIN_OrderDetails_GROUPBY_ShipCountry.orderBy($"ProductSales".desc).show(10) // Top 10 by Sales


}
