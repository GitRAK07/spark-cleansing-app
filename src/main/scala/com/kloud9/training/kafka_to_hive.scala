package com.kloud9.training
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, split}

object kafka_to_hive extends App{

  Logger.getLogger ("org").setLevel (Level.OFF)
  val spark = SparkSession
    .builder ()
    .appName ("Spark Hive Example")
    .master ("local[*]")
    .enableHiveSupport ()
    .getOrCreate()
def read_from_kafka():DataFrame =  {
  val raw_data_kafka: DataFrame = spark
  .readStream
  .format ("kafka")
  .option ("kafka.bootstrap.servers", "localhost:9092")
  .option ("subscribe", "test_topic")
  .option ("startingOffsets", "earliest")
  .load ()
  //.selectExpr ("CAST(value AS STRING)")
  val df_kafka = raw_data_kafka.withColumn("id", split(col("value"), ",").getItem(0))
    .withColumn("firstName", split(col("value"), ",").getItem(1))
    .withColumn("lastName", split(col("value"), ",").getItem(2))
    .withColumn("age", split(col("value"), ",").getItem(3))
    .withColumn("gender", split(col("value"), ",").getItem(4))
    .withColumn("isMarried", split(col("value"), ",").getItem(5))
    .drop("value")
//  df_kafka.writeStream.format("parquet")
//    .option("checkpointLocation", "/Users/anandkumar.r/Desktop")
//    .start("file:///Users/anandkumar.r/Desktop/data_kafka_hive")

//  raw_data_kafka.writeStream.foreachBatch{ (ds: DataFrame, batchId: Long) =>
//    println("Data")
//    raw_data_kafka.show()
//  }.start()

  return df_kafka

}

  read_from_kafka()
    //.write.saveAsTable("k9_nike.kafka_data")

}
