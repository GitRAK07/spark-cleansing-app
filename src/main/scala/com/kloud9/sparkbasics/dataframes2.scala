package com.kloud9.sparkbasics
import org.apache.log4j.{Level, Logger}
import com.kloud9.sparkbasics.dataframes.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import sun.jvm.hotspot.debugger.cdbg.IntType
import spark.implicits._

object dataframes2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("InvoiceProcessor")
    .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
    .config("spark.hadoop.fs.s3a.fast.upload","true")
    .config("spark.sql.parquet.filterPushdown", "true")
    .config("spark.sql.parquet.mergeSchema", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.speculation", "false")
    .getOrCreate()
  // creating a SparkSession for DataFrame
//  val spark = SparkSession.builder()
//    .appName("DataFrames Basics")
//    .config("spark.master", "local")
//    .getOrCreate()
  // Creating DataFrame
  val df = spark.read.option("sep", ' ')
  .option("inferSchema","true").textFile("file:///Users/anandkumar.r/Desktop/fakefriends.csv")
  df.limit(10).show()
  df.printSchema()

  // Create Schema for the DF
  val Schema = StructType(Array(
    StructField("id", IntegerType ),
    StructField("Name", StringType),
    StructField("Age", IntegerType),
    StructField("Friends", IntegerType)
  )
  )
  // Create DF from CSV with Schema Enabled
  val firstDF = spark.read.format("csv").schema(Schema)
  .load("hdfs://localhost:9000/data.txt")

    firstDF.printSchema()
    firstDF.limit(10).show()
    println(firstDF.count())
  //firstDF.filter("Name" like '%s'  )
// Reading through the HDFS
  val secondDF = spark.read.format("csv").schema(Schema)
    .csv("hdfs://localhost:9000/fakefriends.csv")

  //secondDF.filter($"Age" > 30).show()

  secondDF.filter(secondDF("age") > 30).show()

  println("Entering to the SPARK SQL like actual SQL queries")
  // To run a SQL queries , make a DF as a table like below
  secondDF.createOrReplaceTempView("friends")
  val name_ends_with_s = spark.sql("select * from friends where Name like '%s'")

  val final_result= name_ends_with_s.groupBy("Age")
    .sum("Friends").filter(name_ends_with_s("age")<30)
  final_result.show()
  //AWS S3 Configuration - Credentials
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIA6GWKILVXYSJIX6WA")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "L6hwjFd9F2mt6YMADyeqIffijaYkkUlFftaPCNHZ")

  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
  val bucket_path="s3a://data-engineering-k9-rak-1/scalaoutputfromlocal/processed.csv"
  println("Uploading to AWS S3 bucket")
  //Using Coalesce to make all the partition to single partition.
  //Which basically means allowing 1 partition.
  //.repartition(1) also can be used instead of coalesce(1)
  final_result.coalesce(1).write.format("csv").option("header","true")
    .mode("Overwrite").save(bucket_path)
  //Usual Method
  //final_result.write.format("csv").option("header","true").mode("Overwrite").save(bucket_path)
  println("Successfully uploaded to S3")

  // Stopping the spark session
  //spark.stop()


}
