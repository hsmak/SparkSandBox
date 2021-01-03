package com.sparkbyexamples.spark.dataframe.functions.datetime

import com.sparkbyexamples.spark.MyContext
import org.apache.spark.sql.SparkSession

import java.time.{Duration, Instant, LocalDate, Period}

// Alternatively, we can also use case classes
case class MyDate(date: LocalDate)

case class MyStamp(instant: Instant)

/**
  * As of Spark 3, we can use java 8's Date/Time API:
  *   - LocalDate (not LocalDateTime) <- equivalent to current_date()
  *   - Instant                       <- equivalent to current_timestamp()
  */
object Java8DateTimeWithSpark3 extends App with MyContext {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Java8DateTimeWithSpark3")
    .getOrCreate()

  import spark.implicits._

  Seq(
    LocalDate.now,
    LocalDate.now.minusYears(2),
    LocalDate.now.minusMonths(3)
  ).toDF.show(false)

  Seq(
    Instant.EPOCH,
    Instant.now,
    Instant.now.minusMillis(200000),
    Instant.now.plus(Period.ofDays(4)),
    Instant.now.plus(Duration.ofDays(4)),
    //    Instant.EPOCH.plus(Period.ofYears(2))
  ).toDF.show(false)
}
