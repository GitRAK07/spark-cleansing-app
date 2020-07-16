package com.kloud9.training
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object SparkStreamKafka extends DataTransNewDriver {

  def main(args: Array[String]): Unit = {

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .load()

    df.select(col("value"), expr("cast(value as string) as actualValue"))
    .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", "test-topic-output")
      .option("checkpointLocation", "/Users/anandkumar.r/Downloads/csv_files/")
      .start()
      .awaitTermination(10000)



  }

}