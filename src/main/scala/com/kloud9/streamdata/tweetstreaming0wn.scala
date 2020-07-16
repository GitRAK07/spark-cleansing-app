package com.kloud9.streamdata
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
object tweetstreaming0wn {
  def main(args: Array[String]) {
//    if (args.length < 4) {
//      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
//        "[<filters>]")
//      System.exit(1)
//    }
    val appName = "TwitterData"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    val cb = new ConfigurationBuilder
    val consumerKey = "hgfSpKNrzEoP9lxsoycDyw7Ur"
    val consumerSecret = "tFKY2QwBAGi6ZKL8rb50ug3XDYhkQC5atAmDu5LKsFhGTSPRTh"
    val accessToken = "1182938832414494721-2cU6iKSxdcbakXrbh9hEUP6f89TQ6j"
    val accessTokenSecret = "ijCw2YKyNGwzwoZvhWpOXovAVxjrBJknHOFNt1HTrkyj1"

    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    val auth = new OAuthAuthorization(cb.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)
    //tweets.print()
    tweets .saveAsTextFiles("tweets", "json")
    ssc.start()
    ssc.awaitTermination()
  }
}