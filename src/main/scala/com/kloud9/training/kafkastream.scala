package com.kloud9.training
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object kafkastream extends DataTransNewDriver {
  def read_from_kafka() = {

    val df: DataFrame= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
//   .option("auto.offset.reset","latest")
      .load()
    df.writeStream
      .format("console")
      .outputMode("append")
      .start()
    .awaitTermination(100)
    df
      .select(col("*"), expr("cast(value as string) as value"))
      .writeStream
      .format("kafka")
      .option("topic", "test-topic-output")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/Users/anandkumar.r/Downloads/csv_files/")
      .start()


//val mySchema = StructType(Array(
//  StructField("id", StringType),
//  StructField("firstName", StringType),
//  StructField("lastName", StringType),
//  StructField("age", StringType),
//  StructField("gender", StringType),
//  StructField("isMarried", StringType)
//))

//    val df = spark.readStream
//      .schema(mySchema)
//      .option("header","true")
//      .csv("/Users/anandkumar.r/Downloads/csv_files/").toDF()
//
//  df.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//      .awaitTermination()



  }

  def main(args: Array[String]): Unit = {
    read_from_kafka()
  }

}
