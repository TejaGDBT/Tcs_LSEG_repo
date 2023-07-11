package com.spark.code
import org.apache.spark.sql.SparkSession

object SparkSessionObject {
  def createSparkSession(app:String): SparkSession = {
    SparkSession
      .builder()
      .appName("app")
      .master("local[*]") // Set your master URL accordingly
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.instances", "2") // Set the number of executor instances
      .config("spark.executor.cores", "2") // Set the number of cores per executor
      .config("spark.default.parallelism", "8") // Set the default parallelism level
      .config("spark.sql.shuffle.partitions", "8") // Set the number of shuffle partitions
      .getOrCreate()
  }
}