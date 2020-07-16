package com.kloud9.training
//import java.util
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkKafkaStream extends DataTransNewDriver {
  def main(args: Array[String]): Unit = {


    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> classOf[StringSerializer], // send data to kafka
      "value.serializer" -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false.asInstanceOf[Object]
    )

    val kafkaTopic = "test-topic"
    readFromKafkas()

    def readFromKafkas() = {
      val topics = Array(kafkaTopic)
      val kafkaDStream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "test"))
      )
      val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
      processedStream.print()
      ssc.start()
      ssc.awaitTermination()
    }

  }

}