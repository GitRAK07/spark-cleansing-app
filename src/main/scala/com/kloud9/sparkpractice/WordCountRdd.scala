package com.kloud9.sparkpractice
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object WordCountRdd {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkWordCount")
      .getOrCreate()

    //val input = spark.sparkContext.textFile("hdfs://localhost:9000/data/data.txt")

    val df = spark.read.text("hdfs://localhost:9000/data/data.txt")

    df.select("*").show()

  }

}