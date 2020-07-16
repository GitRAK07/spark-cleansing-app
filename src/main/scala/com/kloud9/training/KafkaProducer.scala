package com.kloud9.training
import java.util.Properties
import org.apache.kafka.clients.producer._
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("test-topic-output")
  }
  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic,"10,Emy,Jackson,28,F,Y")
    producer.send(record)
    producer.close()
  }
}
