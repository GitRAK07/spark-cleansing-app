package com.kloud9.sparkstreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark._

object firststream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //  val stream= SparkSession.builder().appName("Stream App")
    //    .master(locally(*))
    //    .getOrCreate()
    //
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))


    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val windowedWordCounts = pairs
      .reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(20), Seconds(10))

//Join Streams
//    val windowedStream1 = pairs.window(Seconds(20))
//    val windowedStream2 = pairs.window(Seconds(10))
//    val joinedStream = windowedStream1.join(windowedStream2)
//    joinedStream.print()

    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    wordCounts.saveAsTextFiles("/Users/anandkumar.r/Desktop/test/test","123")
    println("Svaed")
    //windowedWordCounts.print()
    ssc.start() // Start the computation
    println("The computations started")

    ssc.awaitTermination() // Wait for the computation to terminate

    //ssc.stop()
  }
}