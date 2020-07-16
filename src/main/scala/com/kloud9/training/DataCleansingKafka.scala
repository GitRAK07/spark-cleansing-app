package com.kloud9.training
package com.kloud9.training
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import org.apache.spark.sql.kafka010._
object DataCleansingKafka extends App with DataTransNewDriver {
  readData()
  def readData(): Unit = {
    // reading a DF
    //    val rawdata1 = spark.read
    //      .format("csv")
    //      .option("header", "true")
    //      .load("/Users/anandkumar.r/Downloads/datatransformation.csv")
    //   .toDF()

    val mySchema = StructType(Array(
      StructField("id", StringType),
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("age", StringType),
      StructField("gender", StringType),
      StructField("isMarried", StringType)
    ))
    val rawdata1: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .option("startingOffsets", "latest")
      //.schema(mySchema)
      .load()
    val df = rawdata1.selectExpr("CAST(value AS STRING)")

    println("Raw data from the data lake:")
    val df2 = df.select(from_json(col("value"), mySchema).as("data"))
      .select("data.*")
    //    df2.show()
    //    df2.printSchema()
    println("Results are pushed to the kafka topic")
    val result = df2.transform(columnrename())
      .transform(columnadd())
      .transform(replacecolumnvalue())
      .transform(convertcolumnvalue())
      //.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", "test-topic-output")
      .option("checkpointLocation", "/Users/anandkumar.r/Downloads/csv_files/")
      .start()
      .awaitTermination()


  }

  def columnrename()(df: DataFrame): DataFrame = {
    //Changing the column name of gender to sex
    val modified = df.withColumnRenamed("gender", "sex")
    return modified
  }

  def columnadd()(df: DataFrame): DataFrame = {
    //create a new column called full name by merging columns first name and second name
    val mergedData = df.withColumn("fullName", concat(col("firstName"),
      lit(" "), col("lastName")))
    return mergedData
  }

  def replacecolumnvalue()(df: DataFrame): DataFrame = {
    //Replace values of gender
    val mergedData2 = df.withColumn("sex", when(col("sex")
      .equalTo("M"), "Male").otherwise("Female"))
    return mergedData2
  }
  def updatecolumntype()(df: DataFrame): DataFrame = {
    //Change isMarried type to Boolean
    val mergedData3 = df.withColumn("isMarried", col("isMarried")
      .cast(BooleanType))
    mergedData3.printSchema()
    return mergedData3
  }
  def convertcolumnvalue()(df: DataFrame): DataFrame = {
    //update values of isMarried section to Y --> true, N --> false
    val mergedData4 = df.withColumn("isMarried", when(col("isMarried")
      .equalTo("Y"), "True").otherwise("False"))
    return mergedData4
  }


}
