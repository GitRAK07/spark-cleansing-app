package com.kloud9.training
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class DataTransNewUnitTest extends FunSuite with DataTransNewDriver {

  test("columnrename function validation")
  {
    val dataset = spark.read
      .format("csv")
      .option("header", "true")
      .load("/Users/anandkumar.r/Downloads/datatransformation.csv")
      .toDF()
    dataset.printSchema()
    val operatedDF = dataset.transform(DataTransNew.columnrename())


    val expectedData = Seq (
      Row(1, "anand", "kumar", 20, "M", "Y"),
      Row(2, "kumar", "anand", 30, "F", "N")
    )
    val expectedSchema = List(
      StructField("id", StringType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType,true),
      StructField("age", StringType, true),
      StructField("sex", StringType, true),
      StructField("isMarried", StringType, true)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    assert((operatedDF.schema == expectedDF.schema)==true)

  }

  test("replacecolumnvalue function validation"){
    val dataset = spark.read
      .format("csv")
      .option("header", "true")
      .load("/Users/anandkumar.r/Downloads/datatransformation.csv")
      .toDF()
    val operatedDF = dataset.transform(DataTransNew.replacecolumnvalue())
    operatedDF.printSchema()
    val expectedData = Seq(
      Row(1, "abc", "xxx", 20, "Male", "Y"),
      Row(2, "cbc", "asdf", 30, "Female", "N")
    )
    val expectedSchema = List(
      StructField("id", StringType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("age", StringType, true),
      StructField("sex", StringType, true),
      StructField("isMarried", StringType, true)
    )
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    expectedDF.printSchema()
    if(operatedDF.select("gender") == expectedDF.select("sex"))
    {
      val result = operatedDF.except(expectedDF)
      assert(result.count()==0)
    }
    else{
      println("Inside Else")
      assert (false)
    }
  }


}
