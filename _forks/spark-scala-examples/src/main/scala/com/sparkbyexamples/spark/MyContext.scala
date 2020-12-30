package com.sparkbyexamples.spark

import org.apache.log4j.{Level, Logger}

trait MyContext {
  Logger.getLogger("org").setLevel(Level.OFF)
  val base_dir = s"file://${System.getProperty("user.dir")}"
  val data_dir = base_dir + "/_forks/spark-scala-examples/src/main/resources"
  val out_dir = base_dir + "/_data/OUT"
}
