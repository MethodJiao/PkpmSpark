package spark.bim.kafkaConnect

import org.apache.kafka.clients.consumer.KafkaConsumer

//Kakfa消费者工厂创建接口 为后期定制consumer提供满足开闭的扩展能力
trait ITopicKafka {
  //创建消费者实例
  def CreateConsumer(): KafkaConsumer[String, String]
}
