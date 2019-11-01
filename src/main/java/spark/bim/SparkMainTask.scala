package spark.bim

import java.time.Duration

import com.mongodb.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis
import spark.bim.kafkaConnect.KafkaFactory
import spark.bim.redisConnect.RedisConnector

object SparkMainTask {

  //Mongo数据来源
  def MongodbSearcher(sparkSession: SparkSession): DataFrame = {

    val dataFrame = MongoSpark.load(sparkSession)
    //dataFrame.show()
    dataFrame.createOrReplaceTempView("netflows")
    var resultDataFrame = sparkSession.sql("select name from netflows")
    resultDataFrame = resultDataFrame.groupBy("name").count().orderBy(desc("count"))
    //resultDataFrame.show()
    resultDataFrame
  }

  //写入redis
  def RedisInserter(name: String, redisConnect: Jedis): Unit = {
    redisConnect.lpush("name", name)
  }

  def main(args: Array[String]): Unit = {
    //mongo spark通信槽
    val sparkSession = SparkSession.builder()
      .appName("PKPMBimAnalyse")
      .config("spark.mongodb.input.uri", "mongodb://10.100.140.35/mydb.netflows")
      .getOrCreate()

    val kafkaConsumer = new KafkaFactory().GetKafkaConsumer("order")
    val redisConnect = new RedisConnector().GetRedisConnect()

    //rdd计算部分
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofMillis(100)) //task触发源
      val iterator = records.iterator()
      while (iterator.hasNext) {
        //查mongo
        val resultDateFrame = MongodbSearcher(sparkSession)
        val array = resultDateFrame.collect()
        if (array.length > 0) {
          RedisInserter(array(0)(0).toString, redisConnect)
        }
        iterator.next()
      }
    }
    kafkaConsumer.close()
    redisConnect.close()
    sparkSession.stop()
  }
}
