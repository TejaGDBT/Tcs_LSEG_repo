/*****************************************************************************************************************************************************************
 * ProgramName:TradeExecution                                                                                                                                    *
 * Description:                                                                                                                                                  *
 *             The TradeExecution code is responsible for processing trade orders and matching them between an order book and an order file.                     *
 *             It reads data from CSV files, performs matching operations, and produces output files.                                                           *
 *             The code is implemented using Spark and Scala.                                                                                                   *
 *                                                                                                         																											 *
 ****************************************************************************************************************************************************************/

package com.spark.code
import com.spark.code.TradeExecution.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, rank}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.{Level, Logger}
import scopt._

case class CommandLineArgs(
                            ordersFilePath: String,
                            orderFileName: String,
                            orderBookFilePath: String,
                            orderBookFileName: String,
                            closedOrdersFilePath: String,
                            closedOrdersFileName: String,
                            orderFileArchivalPath: String,
                            orderBookArchivalPath: String
                          )


object TradeExecution {
  val spark = SparkSessionObject.createSparkSession("SparkTradeMatchEngine")
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
  val currentTimeStamp = dateFormat.format(Calendar.getInstance().getTime)

  def parseCommandLineArgs(args: Array[String]): Option[CommandLineArgs] = {
    val parser = new scopt.OptionParser[CommandLineArgs]("TradeExecution") {
      head("TradeExecution", "1.0")

      opt[String]("ordersFilePath").required().action((x, c) => c.copy(ordersFilePath = x)).text("File path where the order files are present")
      opt[String]("orderFileName").required().action((x, c) => c.copy(orderFileName = x)).text("Name of the order file")
      opt[String]("orderBookFilePath").required().action((x, c) => c.copy(orderBookFilePath = x)).text("File path where the order book will be stored")
      opt[String]("orderBookFileName").required().action((x, c) => c.copy(orderBookFileName = x)).text("Order book file name to be created")
      opt[String]("closedOrdersFilePath").required().action((x, c) => c.copy(closedOrdersFilePath = x)).text("Output file path where the closed/matched orders will be stored")
      opt[String]("closedOrdersFileName").required().action((x, c) => c.copy(closedOrdersFileName = x)).text("Output file name for the closed/matched orders")
      opt[String]("orderFileArchivalPath").required().action((x, c) => c.copy(orderFileArchivalPath = x)).text("Path where the input/Order files will be archived after processing")
      opt[String]("orderBookArchivalPath").required().action((x, c) => c.copy(orderBookArchivalPath = x)).text("Path where the order book files will be archived after processing")
    }

    parser.parse(args, CommandLineArgs("", "", "", "", "", "", " ", ""))
  }

  def readCsvFile(filePath: String, Schema: StructType): DataFrame = {
    val fp = new Path(s"$filePath")
    if (fs.exists(fp)) {
      spark.read.format("csv").option("header", "false").schema(Schema).load(filePath)
    }
    else {
      throw new IllegalArgumentException(s"File does not exist: $filePath")
    }
  }

  def archiveFile(sourcefilePath: String, destinationfilePath: String) = {
    val source = new Path(sourcefilePath)
    val destination = new Path(destinationfilePath)
    if (fs.exists(source)) {
      fs.rename(source, destination)
    } else {
      println(s"Source file does not exist: $sourcefilePath")
    }
  }

  //this will merge multiple files present in the source path into a single file in destination path
  def mergeFiles(src: String, dest: String) = {
    if (fs.exists(new Path(src))) {
      FileUtil.copyMerge(fs, new Path(src), fs, new Path(dest), true, spark.sparkContext.hadoopConfiguration, null)
    }
  }

    def matchOrdersBetweenOrderBookAndOrderFile(orderBookDF: DataFrame, orderFileDF: DataFrame): (DataFrame, DataFrame, DataFrame) = {
      // Extract buy orders from the order book and rank them based on quantity and order time
      val orderBookBuyOrdersRankedDF = orderBookDF.filter(col("OrderType") === "BUY")
        .withColumn("rank", rank().over(Window.partitionBy("Quantity").orderBy(col("OrderTime"))))

      // Extract sell orders from the order book and rank them based on quantity and order time
      val orderBookSellOrdersRankedDF = orderBookDF.filter(col("OrderType") === "SELL")
        .withColumn("rank", rank().over(Window.partitionBy("Quantity").orderBy(col("OrderTime"))))


      // Extract buy orders from the order file and rank them based on quantity, order price, and order time
      val orderFileBuyOrdersRankedDF = orderFileDF.filter(col("OrderType") === "BUY")
        .withColumn("rank", rank().over(Window.partitionBy("Quantity")
          .orderBy(col("OrderPrice").desc, col("OrderTime").asc)))

      // Extract sell orders from the order file and rank them based on quantity, order price, and order time
      val orderFileSellOrdersRankedDF = orderFileDF.filter(col("OrderType") === "SELL")
        .withColumn("rank", rank().over(Window.partitionBy("Quantity")
          .orderBy(col("OrderPrice").asc, col("OrderTime").asc)))

      // Join the ranked buy orders from the order book and order file where rank is the same
      val orderBookBuyOrdersMatchedDF = orderBookBuyOrdersRankedDF.as("books")
        .join(orderFileSellOrdersRankedDF.as("file"), Seq("quantity", "rno"))
        .withColumn("match_order_id", when(col("books.order_time") > col("file.order_time"), col("books.order_id")).otherwise(col("file.order_id")))
        .withColumn("first_order_id", when(col("books.order_time") > col("file.order_time"), col("file.order_id")).otherwise(col("books.order_id")))
        .withColumn("order_time_matched", greatest(col("file.order_time"), col("books.order_time")))
        .drop("file.order_time", "books.order_time")
        .select("match_order_id", "first_order_id", "order_time_matched", "quantity", "price")

      // Join the ranked sell orders from the order book and order file where rank is the same
      val orderBookSellOrdersMatchedDF = orderBookSellOrdersRankedDF.as("books")
        .join(orderFileBuyOrdersRankedDF.as("file"), Seq("quantity", "rno"))
        .withColumn("match_order_id", when(col("books.order_time") > col("file.order_time"), col("books.order_id")).otherwise(col("file.order_id")))
        .withColumn("first_order_id", when(col("books.order_time") > col("file.order_time"), col("file.order_id")).otherwise(col("books.order_id")))
        .withColumn("order_time_matched", greatest(col("file.order_time"), col("books.order_time")))
        .drop("file.order_time", "books.order_time")
        .select("match_order_id", "first_order_id", "order_time_matched", "quantity", "price")

      // Combine the matched buy and sell orders
      val orderBookFileOrdersMatchedDF = orderBookBuyOrdersMatchedDF.union(orderBookSellOrdersMatchedDF)
      val closedOrderIdsDF = orderBookFileOrdersMatchedDF.select(array("match_order_id", "first_order_id").as("closedOrdersArray"))
        .withColumn("order_id", explode(col("closedOrdersArray")))
        .select("order_id")
      val unClosedOrdersFromBookDF = orderBookDF.join(closedOrderIdsDF, Seq("order_id"), "left_anti")
      val unClosedOrdersFromFileDF = orderFileDF.join(closedOrderIdsDF, Seq("order_id"), "left_anti")

      (orderBookFileOrdersMatchedDF, unClosedOrdersFromBookDF, unClosedOrdersFromFileDF)
    }

    /* matchOrderWithInTheOrderFile method tries to match the orders with in the orderFile that are left over from matching between orderBook and OrderFile.
     * Then returns the matched and unmatched orders from orderFile
     */

    def matchOrderWithInTheOrderFile(orderFileDF: DataFrame): (DataFrame, DataFrame) = {
      val buyOrdersRankedDF = orderFileDF.where("order_type='BUY'").withColumn("rno", row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
      val sellOrdersRankedDF = orderFileDF.where("order_type='SELL'").withColumn("rno", row_number() over (Window.partitionBy("quantity").orderBy("order_time")))
      val closedOrdersDF = buyOrdersRankedDF.as("buyorders")
        .join(sellOrdersRankedDF.as("sellorders"), col("buyorders.quantity") === col("sellorders.quantity") && col("buyorders.rno") === col("sellorders.rno"))
        .withColumn("match_order_id", when(col("buyorders.order_time") > col("sellorders.order_time"), col("buyorders.order_id")).otherwise(col("sellorders.order_id")))
        .withColumn("first_order_id", when(col("buyorders.order_time") > col("sellorders.order_time"), col("sellorders.order_id")).otherwise(col("buyorders.order_id")))
        .withColumn("order_time_matched", greatest(col("sellorders.order_time"), col("buyorders.order_time")))
        .drop("sellorders.order_time", "buyorders.order_time")
        .withColumn("quantity_matched", col("sellorders.quantity"))
        .withColumn("order_price", when(col("buyorders.order_time") > col("sellOrders.order_time"), col("sellorders.price")).otherwise(col("buyorders.price")))
        .select("match_order_id", "first_order_id", "order_time_matched", "quantity_matched", "order_price")

      val closedOrderIdsDF = closedOrdersDF.select(array("match_order_id", "first_order_id").as("closedOrdersArray"))
        .withColumn("order_id", explode(col("closedOrdersArray")))
        .select("order_id")

      val unClosedOrdersFromFileDF = orderFileDF.join(closedOrderIdsDF, Seq("order_id"), "left_anti")
      (closedOrdersDF, unClosedOrdersFromFileDF)
    }

    //matchOrders function makes calls to the matchOrdersBetweenOrderBookAndOrderFile and matchOrderWithInTheOrderFile methods using orders from order book and order file
    def matchOrders(readOrderBookDF: DataFrame, ordersDF: DataFrame): (DataFrame, DataFrame) = {

      val (closedOrdersFromBook, unclosedOrdersFromBook, unclosedOrdersFromFile) = matchOrdersBetweenOrderBookAndOrderFile(readOrderBookDF, ordersDF)
      val (closedOrdersFromFile, unclosedOrdersFromFileFinal) = matchOrderWithInTheOrderFile(unclosedOrdersFromFile)
      val closedOrdersDF = closedOrdersFromBook.union(closedOrdersFromFile)
      val unClosedOrdersFinalDF = unclosedOrdersFromBook.union(unclosedOrdersFromFileFinal)

      (closedOrdersDF, unClosedOrdersFinalDF)
    }

      def main(args: Array[String]) = {
        try {
          parseCommandLineArgs(args) match {
            case Some(cmdArgs) =>
              val ordersFilePath = cmdArgs.ordersFilePath
              val ordersFileName = cmdArgs.orderFileName
              val orderBookFilePath = cmdArgs.orderBookFilePath
              val orderBookFileName = cmdArgs.orderBookFileName
              val closedOrdersFilePath = cmdArgs.closedOrdersFilePath
              val closedOrdersFileName = cmdArgs.closedOrdersFileName
              val orderFileArchivalPath = cmdArgs.orderFileArchivalPath
              val orderBookArchivalPath = cmdArgs.orderBookArchivalPath

              val ordersSchema = StructType(Array(
                StructField("order_id", StringType),
                StructField("user_name", StringType),
                StructField("order_time", LongType),
                StructField("order_type", StringType),
                StructField("quantity", IntegerType),
                StructField("price", LongType)
              ))

              val ordersDF = readCsvFile(s"$ordersFilePath/$ordersFileName", ordersSchema).persist()
              val orderBookDF = readCsvFile(s"$orderBookFilePath/$orderBookFileName", ordersSchema).persist()
              val (closedOrders, unclosedOrders) = matchOrders(orderBookDF, ordersDF)
              unclosedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$orderBookFilePath/temp_${orderBookFileName}")
              closedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$closedOrdersFilePath/temp_${closedOrdersFileName}")
              archiveFile(s"$ordersFilePath/$ordersFileName", s"$orderFileArchivalPath/$ordersFileName".replace(".csv", s"_$currentTimeStamp.csv"))
              archiveFile(s"$orderBookFilePath/$orderBookFileName", s"$orderBookArchivalPath/$orderBookFileName".replace(".csv", s"_$currentTimeStamp.csv"))
              mergeFiles(s"$orderBookFilePath/temp_${orderBookFileName}", s"$orderBookFilePath/${orderBookFileName}")
              mergeFiles(s"$closedOrdersFilePath/temp_${closedOrdersFileName}", s"$closedOrdersFilePath/${closedOrdersFileName}".replace(".csv", s"_$currentTimeStamp.csv"))
              println(s"Processing Completed.\nTradeMatch output can be found in this path $closedOrdersFilePath ")

            case None =>
              // Invalid command-line arguments, handle the error
              println("Invalid command-line arguments")
          }
        }
        catch {
          case e: Throwable => e.printStackTrace()
        }
        finally {
          spark.stop()
        }
      }
  }