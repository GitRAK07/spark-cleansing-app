package com.kloud9.demok9
import com.kloud9.demok9.Reader._
object objectcall extends Transformation with Writer {
    def main(args: Array[String]): Unit = {
        println("Hello " + Reader.x + "!")
// Data Partitioning calls
   //data_partitioning("k9_nike","review_data","cleaned_data_bucket_review")
// Decommissioned -- Data Duplication removal ( Decommissioned due to straight forward method )
    //data_duplication("k9_nike","data")
// Data Duplication Function wise
   // data_duplication_write(data_duplication_transformation(data_duplication_read("k9_nike","data")),"cleaned_data_bucket")
// Data Duplication part 2
    //  data_duplication_read2("k9_nike","call_service")
// Hive Related calls
  val hive_read = read_from_hive("k9_nike","cleaned_data_bucket")
    val hive_transform =  hive_read.transform(columnrename())
        .transform(columnadd())
        .transform(replacecolumnvalue())
        .transform(convertcolumnvalue())
    //Call write function to write the transformed data to Hive Table
    write_to_hive(hive_transform,"processed_data1")
// Kafka Related calls
  /* val kafka_read = read_from_kafka("test-topic")
    val kafka_transformation = kafka_read.transform(columnrename())
        .transform(columnadd())
        .transform(replacecolumnvalue())
        .transform(convertcolumnvalue())
    //write function to write the transformed data to Kafka Topic
    write_to_kafka(kafka_transformation,"test-topic-output") */
      combine()


    }

}
