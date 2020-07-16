package com.kloud9.sparkbasics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object rddplay  extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  // creating a SparkSession for RDD
  val conf = new SparkConf().setAppName("Basics of RDDs").setMaster("local[*]")
  val sc = new SparkContext(conf)
  // Reading the Data
  val rdd= sc.textFile("file:///Users/anandkumar.r/Desktop/twitter.text")
  println("RDD SHOW TIME")
  //Printing  the RDD data
  rdd.collect().foreach(println)
  println(rdd.count())
  val maprdd= rdd.flatMap(x => x.split('|'))
    println(maprdd.foreach(println))
  //----------------------------------------------------------------------------------------

  //val wordcounts = maprdd.countByValue()

  //val maprdd = rdd.flatMap(line => line.split("|"))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)

//  words = input.flatMap(lambda x: x.split())
//
//  wordCounts = words.countByValue()
//
//  for word, count in wordCounts.items():
//    cleanWord = word.encode('ascii', 'ignore')
//  if (cleanWord):
//  print(cleanWord.decode() + " " + str(count))




  //println(maprdd.collect().foreach(println))
  println(maprdd.collect())



}
