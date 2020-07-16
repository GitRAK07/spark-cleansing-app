package com.kloud9.training
//import com.kloud9.training.DataTransNew.{columnadd, columnrename, convertcolumnvalue, replacecolumnvalue, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataTransNewDriver  {
    // Enable Only the Error Logging
    Logger.getLogger("org").setLevel(Level.ERROR)
    // creating a SparkSession
    val spark = SparkSession.builder()
      .appName("Data massaging")
      .config("spark.master", "local")
      .getOrCreate()
  //val path = "/Users/anandkumar.r/Downloads/datatransformation.csv"
}
