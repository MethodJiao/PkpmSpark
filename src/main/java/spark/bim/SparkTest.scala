package spark.bim

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import java.util.Collections

import com.mongodb.spark._
import org.apache.spark.sql.SparkSession

object SparkTest {

  //kafka消费
  def KafkaConsumer(): Unit = {
    val TOPIC = "flume1"
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "groupA")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(TOPIC))
    while (true) {
      val records = consumer.poll(100)
      records.forEach(record => {
        println(record)
      })
    }
  }

  //Mongo数据来源
  def MongodbSearcher(): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnector")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mydb.netflows")
      .getOrCreate()
    val table = MongoSpark.load(sparkSession)
    //table.show()
    table.createOrReplaceTempView("netflows")
    val resultData = sparkSession.sql("select name from netflows")
    //resultData.show()

    sparkSession.stop()
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    MongodbSearcher()
    conf.setAppName("WordCountLocal")
    conf.setMaster("local")
    val sparkContext = new SparkContext(conf)
    val textFileRDD = sparkContext.textFile("/Library/spark-2.4.4-bin-hadoop2.7/wordsource.txt")
    val wordRDD = textFileRDD.flatMap(line => line.split(" "))
    val pairWordRDD = wordRDD.map(word => (word, 1))
    val wordCountRDD = pairWordRDD.reduceByKey((a, b) => a + b)
    //    wordCountRDD.foreach(keyValue => {
    //      val redisConnect = new Jedis("localhost", 6379, 3000)
    //      redisConnect.lpush("word", keyValue._1)
    //      redisConnect.close()
    //    })
    var arrayBuf = new ArrayBuffer[Int]()
    wordCountRDD.saveAsTextFile("/Library/spark-2.4.4-bin-hadoop2.7/result")
  }
}
