package spark.bim

import com.mongodb.spark._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.bim.redisConnect.RedisConnector
import spark.bim.threeDimensional.{DPoint3D, VolumeAlgorithm}

import scala.collection.mutable

object SparkMainTask {

  //Mongo数据来源
  def MongodbSearcher(sparkSession: SparkSession): DataFrame = {

    val dataFrame = MongoSpark.load(sparkSession)
    dataFrame.createOrReplaceTempView("netflows")
    var resultDataFrame = sparkSession.sql("select * from netflows")
    resultDataFrame = resultDataFrame.distinct()
    //resultDataFrame.show()
    resultDataFrame
  }

  //UDF计算总体积
  def UdfCalculateVolume(highPts: mutable.WrappedArray[GenericRow], lowPts: mutable.WrappedArray[GenericRow]): Double = {
    val lowAndHighZip = highPts.zip(lowPts)
    var totalVolume = 0.0
    for (lowAndHigh <- lowAndHighZip) {
      val highPt = lowAndHigh._1.toSeq
      val lowPt = lowAndHigh._2.toSeq
      val highPt3D: DPoint3D = new DPoint3D(highPt.head.##, highPt.apply(1).##, highPt.apply(2).##)
      val lowPt3D = new DPoint3D(lowPt.head.##, lowPt.apply(1).##, lowPt.apply(2).##)
      totalVolume += VolumeAlgorithm.OrthogonalVolume(highPt3D, lowPt3D)
    }
    totalVolume
  }


  //哈希算法
  def HashString(str: String): Long = {
    var hashCode: Long = 0
    val bytes = str.getBytes()
    for (index <- 0 until bytes.size) {
      hashCode = 5 * hashCode + bytes.apply(index)
    }
    hashCode
  }

  def main(args: Array[String]): Unit = {
    //mongo spark通信槽
    val sparkSession = SparkSession.builder()
      .appName("PKPMBimAnalyse")
      .config("spark.mongodb.input.uri", "mongodb://10.100.140.35/mydb.netflows")
      .master("local")
      .getOrCreate()

    //UDF注册
    val UCalculateVolume = udf(UdfCalculateVolume _)

    var resultDataFrame = MongodbSearcher(sparkSession)
    //新增列TotalVolume计算总体积 TotalJson整行json数据
    resultDataFrame = resultDataFrame.withColumn("TotalVolume", UCalculateVolume(col("RootNode.ChildNode.HighPt"), col("RootNode.ChildNode.LowPt")))
    resultDataFrame = resultDataFrame.withColumn("TotalJson", to_json(col("RootNode")))
    resultDataFrame = resultDataFrame.select(col("RootNode.ChildNode.HighPt"), col("RootNode.ChildNode.LowPt"), col("TotalVolume"), col("TotalJson"))
    resultDataFrame.show()

    val redisConnect = new RedisConnector().GetRedisConnect()
    //算最大交叠
    val dataFrame = resultDataFrame.collect()
    for (tableRow1 <- dataFrame) {
      val cubeList1 = VolumeAlgorithm.GetCube3DList(tableRow1)
      for (tableRow2 <- dataFrame) {
        //同表循环2
        if (tableRow1 != tableRow2) {
          val cubeList2 = VolumeAlgorithm.GetCube3DList(tableRow2)
          //开始计算最大交叠
          val totalPercent = VolumeAlgorithm.CalculatePercent(cubeList1, cubeList2, tableRow1.getAs[Double](2), tableRow2.getAs[Double](2))
          if (totalPercent > 0.8) {
            redisConnect.lpush("Result", tableRow1.getAs[String](3))
          }
        }
      }
    }
    resultDataFrame.show()
    //    resultDataFrame = resultDataFrame.withColumn("Percent", UCalculatePercent(col("HighPt"), col("LowPt"), col("TotalVolume")))
    //    resultDataFrame.show()

    //        resultDataFrame.foreach(document => {
    //          val documentSize = document.size
    //          for (listIndex <- 0 until documentSize)
    //            document.getList(listIndex).toArray.foreach(line => {
    //              print(line)
    //            })
    //        })

    //    val kafkaConsumer = new KafkaFactory().GetKafkaConsumer("order")
    //    val redisConnect = new RedisConnector().GetRedisConnect()
    //
    //    //rdd计算部分
    //    while (true) {
    //      val records = kafkaConsumer.poll(Duration.ofMillis(100)) //task触发源
    //      val iterator = records.iterator()
    //      while (iterator.hasNext) {
    //        //查mongo
    //        val resultDateFrame = MongodbSearcher(sparkSession)
    //        val array = resultDateFrame.collect()
    //        if (array.length > 0) {
    //          RedisInserter(array(0)(0).toString, redisConnect)
    //        }
    //        iterator.next()
    //      }
    //    }
    //    kafkaConsumer.close()
    //    redisConnect.close()
    redisConnect.close()
    sparkSession.stop()
  }
}

