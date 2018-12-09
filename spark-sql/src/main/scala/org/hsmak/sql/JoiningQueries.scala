package org.hsmak.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JoiningQueries extends App {

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


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/fdps-v3-master"


  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
    .appName("DatasetRunner")
    .getOrCreate()


  import spark.implicits._


  /** ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val employeesDF: DataFrame = spark.read
    .option("header", "true")
    .csv(s"$base_data_dir/data/NW/NW-Employees.csv")


  val ordersDF = spark.read.option("header", "true").
    csv(s"$base_data_dir/data/NW/NW-Orders.csv")


  val orderDetailsDF = spark.read.option("header", "true").
    option("inferSchema", "true").
    csv(s"$base_data_dir/data/NW-Order-Details.csv")


  val productsDF = spark.read.option("header", "true").
    option("inferSchema", "true").
    csv(s"$base_data_dir/data/NW-Products.csv")


  /** ****************************************************
    * ############ Creating Datasets from DF ###########
    * ****************************************************/


  //Employees
  val employees = employeesDF.as[Employee] // binding to a type

  println("Employees has " + employees.count() + " rows")
  employees.show(5)
  println(employees.head())
  employees.dtypes.foreach(println) // verify column types


  //Orders
  val orders = ordersDF.as[Order]

  println("Orders has " + orders.count() + " rows")
  orders.show(5)
  println(orders.head())
  orders.dtypes.foreach(println)


  //OrderDetails
  val orderDetails = orderDetailsDF.as[OrderDetails]

  println("Order Details has " + orderDetails.count() + " rows")
  orderDetails.show(5)
  println(orderDetails.head())
  orderDetails.dtypes.foreach(println) // verify column types


  //Products
  val products = productsDF.as[Product]

  println("Order Details has " + products.count() + " rows")
  products.show(5)
  println(products.head())
  products.dtypes.foreach(println) // verify column types


  /** ******************************************************
    * ################# Creating Views/Tables ##############
    * ******************************************************/


  employees.createOrReplaceTempView("EmployeesTable")
  orders.createOrReplaceTempView("OrdersTable")
  orderDetails.createOrReplaceTempView("OrderDetailsTable")
  products.createOrReplaceTempView("ProductsTable")


  /** ******************************************************
    * ################# SQL Operations #####################
    * *******************************************************/


  val Orders_JOIN_OrderDetails = spark.sql(

    """SELECT OrderDetailsTable.OrderID, ShipCountry, UnitPrice, Qty, Discount
      |FROM OrdersTable
      |INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID""".stripMargin)

  Orders_JOIN_OrderDetails.show(10)
  Orders_JOIN_OrderDetails.head(3).foreach(println)


  // Sales By Country

  val Sales_GROUPEDBY_ShipCountry = spark.sql(

    """SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales
      |FROM OrdersTable
      |INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID
      |GROUP BY ShipCountry""".stripMargin)

  println(Sales_GROUPEDBY_ShipCountry.count())
  Sales_GROUPEDBY_ShipCountry.show(100)
  Sales_GROUPEDBY_ShipCountry.head(3).foreach(println)

  Sales_GROUPEDBY_ShipCountry.orderBy($"ProductSales".desc).show(10) // Top 10 by Sales


  /**
    * ToDO:
    *   - "The Dataset also includes the product table, which I leave to you as an exercise. For example, you can work on a query that returns sales by product or one that shows the products that are selling more. The Dataset also has date fields, such as order dates, which you can use to query sales by quarters, and reports, such as Product sales for 1997. The dates are now read in as strings. They need to be converted to the            TIMESTAMP data type."
    *
    */

  //Sales GroupedBy Products
  val Sales_GROUPEDBY_Products = spark.sql(

    """SELECT ProductName, SUM(OrderDetailsTable.Qty) AS ProductSales
      |FROM ProductsTable
      |
      |INNER JOIN OrderDetailsTable ON ProductsTable.ProductID = OrderDetailsTable.ProductID
      |
      |GROUP BY ProductName
      |ORDER BY ProductSales DESC""".stripMargin)

  Sales_GROUPEDBY_Products.show

  /**
    * for date/timestamp ops, refer to: https://spark.apache.org/docs/latest/api/sql/index.html#timestamp
    *
    * ```
    * trunc(to_date(OrdersTable.OrderDate, 'dd/mm/yy'), 'yyyy')
    * ```
    *
    */


  //Sales GroupedBy Products for 1997
  val Sales_GROUPEDBY_Products_1997 = spark.sql(

    """SELECT ProductName, SUM(OrderDetailsTable.Qty) AS ProductSales_97
      |FROM ProductsTable
      |
      |INNER JOIN OrderDetailsTable ON ProductsTable.ProductID = OrderDetailsTable.ProductID
      |INNER JOIN OrdersTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID
      |
      |WHERE trunc(to_date(OrdersTable.OrderDate, 'dd/mm/yy'), 'yyyy') = trunc('1997', 'yyyy')
      |
      |GROUP BY ProductName
      |ORDER BY ProductSales_97 DESC""".stripMargin)

  Sales_GROUPEDBY_Products_1997.show
}
