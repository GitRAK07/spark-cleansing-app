package com.kloud9.demok9
import com.kloud9.training.DataTransNew.{columnadd, columnrename, convertcolumnvalue, replacecolumnvalue, spark}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, from_json, lit, split, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object Reader extends sessions {

    val x = "Anand"

  def combine():DataFrame ={
    val df1 = spark.sql("select * from k9_nike.data")
//    val df2 = spark.read
//        .format("csv")
//      .option("header","true")
//      .load("file:///Users/anandkumar.r/Desktop/test.csv")
//      .toDF()
    val df2 = spark.sql("select * from k9_nike.data_test")
    df1.show()
    df2.show()
    val df3 = spark.sql("select df1.id,firstname,lastname,age,ismarried,gender_value as gender from k9_nike.data as df1 left join k9_nike.data_test as df2 on df1.gender=df2.gender")
    df3.show()
    //val df4 = df3.withColumnRenamed("gender_value", "gender")
    //df4.show()
    df3.createOrReplaceTempView("table")
    spark.sql("select * from table where gender is NOT NULL").show()
    spark.sql("select * from table where gender is NULL").show()
   // val df3 = df1.withColumn("gender", when(col("gender")
//      .equalTo(df2.select("gender")), df2.select("isMarried"))
//      .otherwise("None"))
//    df3.show()
//    val df3 = df1.join(df2)
//    df3.show()
    //val df3 = df1.withColumn("fullName", df2 )

    return df2

  }

    def read_from_hive(db_name: String , input_table: String): DataFrame = {
     // spark.sql("show databases").show()
      spark.sql(s"use ${db_name}")
      val df_hive: DataFrame = spark.sql(s"select * from ${input_table}")
      println("The initial RAW data from HIVE TABLE - 'data'")
      df_hive.show()
      return df_hive

    }
    def read_from_kafka(input_topic: String): DataFrame = {
    // Read messages from Kafka Topic
      val raw_data_kafka: DataFrame = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING)")
      // Split column "value" into multiple Columns
      val df_kafka = raw_data_kafka.withColumn("id", split(col("value"), ",").getItem(0))
        .withColumn("firstName", split(col("value"), ",").getItem(1))
        .withColumn("lastName", split(col("value"), ",").getItem(2))
        .withColumn("age", split(col("value"), ",").getItem(3))
        .withColumn("gender", split(col("value"), ",").getItem(4))
        .withColumn("isMarried", split(col("value"), ",").getItem(5))
        .drop("value")
     return df_kafka
    }
    def data_duplication_read(db_name: String , raw_table: String): DataFrame = {
      spark.sql(s"use ${db_name}")
      val df_duplicate:DataFrame = spark.sql(s"select * from ${raw_table} ")
      println("Original data :")
      df_duplicate.show()
      return df_duplicate
    }

  def data_duplication_read2(db_name: String , raw_table: String): DataFrame = {
    spark.sql(s"use ${db_name}")
    val df_duplicate:DataFrame = spark.sql(s"select * from ${raw_table} ")
    println("Original data :")
    df_duplicate.show()

    val dup = spark.sql("select callDateTime,priority,district,callNumber,incidentLocation,location,count(*) from call_service group by callDateTime,priority,district,callNumber,incidentLocation,location having count(*) > 1")
    dup.show()
    return df_duplicate
  }


}
