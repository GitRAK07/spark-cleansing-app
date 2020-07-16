package com.kloud9.training
import org.apache.spark.sql.functions._
//import com.kloud9.sparkbasics.dataframes.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.BooleanType

object DataTranformation {

  def main(args: Array[String]) = {

    // Enable Only the Error Logging
    Logger.getLogger("org").setLevel(Level.ERROR)
    // creating a SparkSession
    val spark = SparkSession.builder()
      .appName("Data massaging")
      .config("spark.master", "local")
      .getOrCreate()

    // reading a DF
    val rawdata = spark.read
      .format("csv")
      .option("header", "true")
      .load("/Users/anandkumar.r/Downloads/datatransformation.csv")
      .toDF()

    //firstDF.show()
    //firstDF.printSchema()

    //Changing the column name of gender to sex
    val modified = rawdata.withColumnRenamed("gender", "sex")

    //modified.printSchema()
    //modified.show()

    //create a new column called full name by merging columns first name and second name
    val mergedData=modified.withColumn("fullName",concat(col("firstName"),
      lit(" "),col("lastName")))
    //mergedData.show()

    //Replace values of gender
    val mergedData2=mergedData.withColumn("sex",when(col("sex")
      .equalTo("M"),"Male").otherwise("Female"))
    mergedData2.printSchema()

    //Change isMarried type to Boolean
    val mergedData3=mergedData2.withColumn("isMarried",col("isMarried")
      .cast(BooleanType))
    mergedData3.printSchema()

    //update values of isMarried section to Y --> true, N --> false
    val mergedData4=mergedData3.withColumn("isMarried",when(col("isMarried")
        .equalTo("Y"),"True").otherwise("False"))
    mergedData4.show()

    //mergedData4.write.format("csv").option("header","true")
      //.save("/Users/anandkumar.r/Downloads/ProcessedData.csv")
    spark.stop()
  }
}


