package spark.bim

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
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
    wordCountRDD.saveAsTextFile("/Library/spark-2.4.4-bin-hadoop2.7/result")
  }
}
