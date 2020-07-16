package com.kloud9.training
import org.apache.spark.sql.{DataFrame,SaveMode, SparkSession}
import org.apache.spark.sql.hive._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.types.BooleanType
object hivedata extends App  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
   spark.catalog.listDatabases().show(true)
    import spark.sql

    sql("show databases").show()
    sql("use k9_nike")
    val df: DataFrame = spark.sql("select * from data")

  //Tranformation of the values
    val result =  df.transform(columnrename())
      .transform(columnadd())
      .transform(replacecolumnvalue())
      .transform(convertcolumnvalue())
  //Storing the result into hive data( mysql for metastore and hdfs for storage)
  result.write.mode(SaveMode.Overwrite).saveAsTable("processed_data")

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


