package spark.bim.kafkaConnect

import org.apache.kafka.clients.consumer.KafkaConsumer

class KafkaFactory {
  //kafka配置
  def GetKafkaConsumer(topicName: String): KafkaConsumer[String, String] = {

    topicName match {
      case "order" =>
        val orderTopicKafka = new OrderTopicKafka()
        return orderTopicKafka.CreateConsumer()
    }
    null
  }
}
