package com.kloud9.demok9
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.types.BooleanType
trait Transformation {
  //Changing the column name of gender to sex
  def columnrename()(df: DataFrame): DataFrame = {
    val modified = df.withColumnRenamed("gender", "sex")
    return modified
  }
  //create a new column called full name by merging columns first name and second name
  def columnadd()(df: DataFrame): DataFrame = {
    val mergedData = df.withColumn("fullName", concat(col("firstName"),
                        lit(" "), col("lastName")))
    return mergedData
  }

  //Replace values of gender
  def replacecolumnvalue()(df: DataFrame): DataFrame = {
    val mergedData2 = df.withColumn("sex", when(col("sex")
                        .equalTo("M"), "Male").otherwise("Female"))
    return mergedData2
  }
  //Change isMarried type to Boolean
  def updatecolumntype()(df: DataFrame): DataFrame = {
    val mergedData3 = df.withColumn("isMarried", col("isMarried")
                        .cast(BooleanType))
    mergedData3.printSchema()
    return mergedData3
  }
  //update values of isMarried section to Y --> true, N --> false
  def convertcolumnvalue()(df: DataFrame): DataFrame = {
    val mergedData4 = df.withColumn("isMarried", when(col("isMarried")
                        .equalTo("Y"), "True").otherwise("False"))
    return mergedData4
  }
  def data_duplication_transformation(df:DataFrame): DataFrame = {
  //Drop the rows with null values
    println("Rows with null values are dropped")
    val null_rows_drop = df.na.drop()
    null_rows_drop.show()
  //Drop duplicate rows based on the column id

    val dupl_rows_drop = null_rows_drop.dropDuplicates("id")
    println("Duplicate rows are dropped")
    dupl_rows_drop.show(false)
  //Convert the Integer columns with Negative Values to Positive Values
    val convert_positive = dupl_rows_drop.selectExpr("id", "firstname", "lastname", "abs(age) as age", "gender", "isMarried")
    println("The columns with values negative are converted to positive")
    convert_positive.show()
    return convert_positive
  }
}
