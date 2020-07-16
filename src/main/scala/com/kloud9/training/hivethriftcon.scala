package com.kloud9.training
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
object hivethriftcon extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession
    .builder()
    .appName("Spark Hive Resources")
    .master("local[*]")
   // .config("hive.metastore.uris","thrift://localhost:9083")
    //.config("hive.server2.thrift.port","10000")
    .enableHiveSupport()
    .getOrCreate()
  spark.sql("show databases").show()
 // spark.sql("create database kloud9_nike3")

  //spark.sql("show tables")
}



