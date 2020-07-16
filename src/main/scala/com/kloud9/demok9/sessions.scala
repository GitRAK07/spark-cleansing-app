package com.kloud9.demok9
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
trait sessions {
    Logger.getLogger ("org").setLevel (Level.OFF)
    val spark = SparkSession
    .builder ()
    .appName ("Spark Hive Example")
    .master ("local[*]")
    .enableHiveSupport()
      .config("spark.sql.crossJoin.enabled","true")
//      .config("spark.sql.hive.convertMetastoreParquet","false")
//      .config("javax.jdo.option.ConnectionURL","jdbc:mysql://localhost/metastore_?createDatabaseIfNotExist=true")
//      .config("javax.jdo.option.ConnectionDriverName","com.mysql.jdbc.Driver")
//      .config("javax.jdo.option.ConnectionUserName","root")
//      .config("javax.jdo.option.ConnectionPassword","root")
//      .config("hive.metastore.schema.verification","false")
//    .config("spark.hadoop.datanucleus.autoCreateSchema","false")
//    .config("spark.hadoop.datanucleus.fixedDatastore","false")
    .getOrCreate ()

    spark.sparkContext.setLogLevel ("ERROR")

}
