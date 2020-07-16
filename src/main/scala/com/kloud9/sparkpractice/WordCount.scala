package com.kloud9.spark.demo
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkWordCount")
      .getOrCreate()

    //    val input = spark.sparkContext.textFile("hdfs://localhost:9000/data/data.txt")

    //    val df = spark.read.text("hdfs://localhost:9000/data/data.txt")

    //    df.select("*").show()


    val year = LocalDate.now().format(DateTimeFormatter.ofPattern("YYYY"))
    val date = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))
    val month = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))

    val fileName="friends.csv"
    val rawFolderName ="raw"
    val s3BucketName = "data-engineering-k9-rak"

    println(year)
    println(date)
    println(month)

    val path = s3BucketName +"/" + year+"/" +month +"/" +date +"/" + rawFolderName +"/" + fileName

    println(year)
    println(month)
    println(date)

    println(path)


    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIA6GWKILVXZMFUA3YK")
    // Replace Key with your AWS secret key (You can find this on IAM
    println("success Access key")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "jGyt1ypZCNwfnAanrs5ffZBzQcJ1E2EBu8pQwYc7")
    println("success Secret key")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
print("Success")
    val s3Url = "s3a://"+path

    //    val input = spark.sparkContext.textFile(s3Url)
print(s3Url)
    val df = spark.read.csv(s3Url)
    print("success")
    df.select("*").show(100)

    //val input = spark.read.csv(s3Url)



  }

}
