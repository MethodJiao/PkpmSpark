package spark.bim

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import java.util.Collections
import org.apache.spark.sql.functions._
import com.mongodb.spark._
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

object SparkTest {

  //kafka消费 spark子触发源
  def KafkaConsumer(consumer: KafkaConsumer[String, String]): ConsumerRecords[String, String] = {
    val records = consumer.poll(100)
    return records
  }

  //Mongo数据来源
  def MongodbSearcher(sparkSession: SparkSession): DataFrame = {

    val dataFrame = MongoSpark.load(sparkSession)
    //dataFrame.show()
    dataFrame.createOrReplaceTempView("netflows")
    var resultDataFrame = sparkSession.sql("select name from netflows")
    resultDataFrame = resultDataFrame.groupBy("name").count().orderBy(desc("count"))
    //resultDataFrame.show()
    return resultDataFrame
  }

  //写入redis
  def RedisInserter(name: String): Unit = {
    val redisConnect = new Jedis("localhost", 6379, 3000)
    redisConnect.lpush("name", name)
    redisConnect.close()
  }

  def main(args: Array[String]): Unit = {
    //mongo spark消息槽
    val sparkSession = SparkSession.builder()
      .appName("MongoSparkConnector")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydb.netflows")
      .getOrCreate()
    //kafka配置
    val TOPIC = "flume1"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "groupA")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(TOPIC))

    while (true) {
      val records = KafkaConsumer(consumer)

      val iterator = records.iterator()
      //      //判断是否还有数据 结果:12345
      while (iterator.hasNext) {
        //查mongo
        val resultDateFrame = MongodbSearcher(sparkSession)
        val array = resultDateFrame.collect()
        if (array.size > 0) {
          RedisInserter(array(0)(0).toString())
        }
        iterator.next()
      }
    }
    sparkSession.stop()
//    val conf = new SparkConf()
//    //MongodbSearcher()
//    conf.setAppName("WordCountLocal")
//    conf.setMaster("local")
//    val sparkContext = new SparkContext(conf)
//    val textFileRDD = sparkContext.textFile("/Library/spark-2.4.4-bin-hadoop2.7/wordsource.txt")
//    val wordRDD = textFileRDD.flatMap(line => line.split(" "))
//    val pairWordRDD = wordRDD.map(word => (word, 1))
//    val wordCountRDD = pairWordRDD.reduceByKey((a, b) => a + b)
//    wordCountRDD.foreach(keyValue => {
//      val redisConnect = new Jedis("localhost", 6379, 3000)
//      redisConnect.lpush("word", keyValue._1)
//      redisConnect.close()
//    })
//    var arrayBuf = new ArrayBuffer[Int]()
//    wordCountRDD.saveAsTextFile("/Library/spark-2.4.4-bin-hadoop2.7/result")
  }
}
