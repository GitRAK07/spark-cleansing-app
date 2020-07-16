package com.kloud9.demok9
import com.kloud9.demok9.Reader.spark
import org.apache.spark.sql.{DataFrame, SaveMode}
trait Writer  {

  def write_to_hive(df_output_hive: DataFrame,output_table: String){
    //Storing the result into hive data( mysql for metastore and hdfs for storage)
    df_output_hive.write.mode(SaveMode.Overwrite).saveAsTable(output_table)
    spark.sql(s"select * from ${output_table}").show()
    println("---------------------------------------------------------------------")
    println("The Data has been transformed and written to the Hive_Table as above")
    println("---------------------------------------------------------------------")
    //return df
  }
  def data_partitioning(db_name: String,raw_table: String,result_table: String)
  {
    //Setting the config value of hive dynamic paritition to nonstrict mode to avoid static partition
    spark.sql(" set hive.exec.dynamic.partition.mode=nonstrict")
    //Database operation
    spark.sql(s"use ${db_name}")
    println("Connected to the database " + db_name)
    val df=spark.sql(s"select * from ${raw_table} ")
    //Assigning the time
    val t1 = System.nanoTime
    // val df1 = df.coalesce(1)
    //Repartitioning the DataFrame as a single piece of file
    val df1 = df.repartition(1)
    //Writing the Data to the table with Partitioning enabled
    df1.write
      .format("json")
    // .bucketBy(1, "productid")
      .partitionBy("score")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${result_table}")
    println(s"Data written to the table ${result_table}")
    //Finding the overall time taken for the process
    val duration1 = (System.nanoTime - t1) / 1e9d
    println("Time taken  is ",duration1,"s")
//    Computation with partitioning for data read operation
    val t11 = System.nanoTime
    //Reading the data from the partitioned table
    spark.sql(s"select * from ${result_table} where score=5").show()
    val duration11 = (System.nanoTime - t11) / 1e9d
    println("Time taken  is ",duration11,"s")

    //Computation without partitioning for data write operation
    val t2 = System.nanoTime
   // val df2 = df.repartition(1)
    df.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .saveAsTable("cleaned_data_bucket_review1")
    val duration2 = (System.nanoTime - t2) / 1e9d
    println("Time taken  is ",duration2,"s")
    //Normal Computation without partitioning for data read operation
    val t22 = System.nanoTime
    spark.sql("select * from cleaned_data_bucket_review1 where score=5").show()
    val duration22 = (System.nanoTime - t22) / 1e9d
    println("Time taken  is ",duration22,"s")
    println("----------------------------------")
    println("Data Partitioning is now completed")
    println("----------------------------------")
  }
  def data_duplication_write(df_output_cleansed:DataFrame,output_table: String){
    df_output_cleansed.write
      .bucketBy(3, "id")
      .partitionBy("age")
      .mode(SaveMode.Overwrite)
      .saveAsTable(output_table)
    println("Final result stored in the table")
    spark.sql(s"select * from ${output_table} order by id").show()
    println("-----------------------------------------")
    println("Data Duplication and NULL values are removal is now completed")
    println("-----------------------------------------")
  }
// Decommissioned function as it is now divided into pieces of code
//Decommissioned due to straight forward method
  def data_duplication(db_name: String , raw_table: String) {
    spark.sql(s"use ${db_name}")
    val df = spark.sql(s"select * from ${raw_table} ")
    df.show()
    println("Rows with null values are dropped")
    val null_rows_drop = df.na.drop()
    null_rows_drop.show()
    val dupl_rows_drop = null_rows_drop.dropDuplicates("id")
    println("Duplicate rows are dropped")
    dupl_rows_drop.show(false)
    val convert_positive = dupl_rows_drop.selectExpr("id","firstname","lastname","abs(age) as age","gender","isMarried")
    println("The columns with values negative are converted to positive")
    convert_positive.show()
    convert_positive.write
      .bucketBy(3, "id")
      .partitionBy("age")
      .mode(SaveMode.Overwrite)
      .saveAsTable("cleaned_data_bucket")
    spark.sql("select * from cleaned_data_bucket order by id").show()
    println("-----------------------------------------")
    println("Data Duplication removal is now completed")
    println("-----------------------------------------")
  }
  def write_to_kafka(df_kafka_output: DataFrame,output_topic: String){

    println("Processing the Latest Kafka Messages on the topic - 'test-topic'")
    println("NOTE: Click on Stop button to end the Spark-Kafka Streaming process")
    df_kafka_output.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic",output_topic)
      .option("checkpointLocation", "/Users/anandkumar.r/Downloads/csv_files/")
      .start()
      .awaitTermination()
    println("Written to kafka")
  }

}
