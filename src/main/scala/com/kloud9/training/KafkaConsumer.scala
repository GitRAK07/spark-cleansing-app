package com.kloud9.training
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("test-topic-output")
  }
  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset","latest")
    props.put("enable.auto.commit" , (false: java.lang.Boolean))
    props.put("seekToBeginning","true")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("auto.offset.reset", "latest")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    //println(consumer)
    println(consumer.subscribe(util.Arrays.asList(topic)))
    while (true) {
     val record = consumer.poll((100)).asScala
      for (data <- record.iterator) {


        println(data.value())

      }

    }
  }
  }
