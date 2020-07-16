package com.kloud9.streamdata
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import Utilities._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(10))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()
    println(System.getProperty("twitter4j.oauth.consumer_key"))
    println(System.getProperty("twitter4j.oauth.consumerKey"))
    println(System.getProperty("twitter4j.oauth.consumerSecret"))
    println(System.getProperty("twitter4j.oauth.accessToken"))
    println(System.getProperty("twitter4j.oauth.accessTokenSecret"))
    //println(System.getenv())

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    println("Success")
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    println("Success2")
    // Print out the first ten
    statuses.print()
    println("Success3")
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }
}
