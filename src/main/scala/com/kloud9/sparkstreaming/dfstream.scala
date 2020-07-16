package com.kloud9.sparkstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object dfstream {
  Logger.getLogger("org").setLevel(Level.ERROR)
  var spark = SparkSession.builder()
    .master("local[*]")
    .appName("Dataframe streaming")
    .getOrCreate()

  def read_from_socket() = {

    var lines: DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","1234")
      .load()

    //Transformation
    var shortLines : DataFrame = lines.filter(length(col("value") ) <= 5 )

    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    read_from_socket()
  }
}
