package spark.bim.kafkaConnect

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer

class OrderTopicKafka extends ITopicKafka {

  override def CreateConsumer(): KafkaConsumer[String, String] = {
    //kafka配置
    val topic = "order"
    val props = new Properties()
    props.put("bootstrap.servers", "10.100.140.35:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "spark")
    val kafkaConsumer = new KafkaConsumer[String, String](props)
    kafkaConsumer.subscribe(Collections.singletonList(topic))
    kafkaConsumer
  }
}
