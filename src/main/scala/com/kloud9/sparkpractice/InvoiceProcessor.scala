package com.kloud9.sparkpractice

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.lang.Math
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
object InvoiceProcessor {

  def main(args: Array[String]): Unit = {

    //Setup Logging
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Setup Spark Session
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



    //Preparing S3 Work Path

    val year  = LocalDate.now().format(DateTimeFormatter.ofPattern("YYYY"))

    val date = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))


    val month = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))

    val fileName="sales.csv"
    val rawFolderName ="raw"
    val processedFolderName ="processed"
    val s3BucketName = "data-engineering-k9-rak-1"

    //val inputPath = s3BucketName +"/" + year+"/" +month +"/" +date +"/" + rawFolderName +"/" + fileName
    val outputPath = s3BucketName +"/" + year+"/" +month +"/" +date +"/" + processedFolderName +"/output"

    val inputPath ="data-engineering-k9-rak-1/2020/04/30/raw/twitter.text"
    val s3InputUrl = "s3a://"+inputPath
    val s3OutputUrl = "s3a://"+outputPath

    println(s3InputUrl)

    println(s3OutputUrl)

    //AWS S3 Configuration - Credentials
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIA6GWKILVXYSJIX6WA")
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "L6hwjFd9F2mt6YMADyeqIffijaYkkUlFftaPCNHZ")
    // Endpoint of S3. Make Sure you use us-west-2 bucket
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    import org.apache.spark.sql.functions.split
    //val sqlContext = new org.apache.spark.sql.SQLContext(spark)
   // import sqlContext.implicits._
    import org.apache.spark.sql._
//    import spark.implicits._
//    val schm = StructType(
//      StructField("Column1", StringType, nullable = true) ::
//        StructField("Column2", StringType, nullable = true) ::
//        StructField("Column3", StringType, nullable = true) :: Nil)



    //  val rdd= spark.read.schema(schm).csv(s3InputUrl)

        //option("delimiter", '|').schema(schm).csv(s3InputUrl).toDF()

//    rdd.show()
//    rdd.printSchema()

     // val rddd= rdd.map(x => x.split("|")).toDF("date","id","tweets")
    //rddd.show()
//    rdd.printSchema()
//    rdd.show()
//    val df = rdd.toDF()
//    println(df.collect())
//    df.printSchema()
    //df.show()
    //val dff=df.select("*").limit(2).show()





//    val abc=df.withColumn("value", split($"columnToSplit", "|")).select(
//      $"value".getItem(0).as("date"),
//      $"value".getItem(1).as("id"),
//      $"value".getItem(2).as("tweet")
//    )
//  abc.show()

    println("Process Completed")


  }

}