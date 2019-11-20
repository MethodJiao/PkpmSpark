package spark.bim

import com.mongodb.spark._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis
import spark.bim.redisConnect.RedisConnector
import spark.bim.threeDimensional.{Cube3D, DPoint3D, VolumeAlgorithm}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  //写入redis
  def RedisInserter(name: String, redisConnect: Jedis): Unit = {
    redisConnect.lpush("Result", name)
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

  //获取单条数据内的立方体合集
  def GetCube3DList(row: Row): ArrayBuffer[Cube3D] = {
    val highPts: mutable.WrappedArray[GenericRow] = row.getAs[mutable.WrappedArray[GenericRow]](0)
    val lowPts: mutable.WrappedArray[GenericRow] = row.getAs[mutable.WrappedArray[GenericRow]](1)
    val lowHighZip = highPts.zip(lowPts)
    val cubeList = ArrayBuffer.empty[Cube3D] //单条数据内所有的cube
    lowHighZip.foreach(lowHigh => {
      val highPt = lowHigh._1.toSeq
      val lowPt = lowHigh._2.toSeq
      val highPt3D: DPoint3D = new DPoint3D(highPt.head.##, highPt.apply(1).##, highPt.apply(2).##)
      val lowPt3D = new DPoint3D(lowPt.head.##, lowPt.apply(1).##, lowPt.apply(2).##)
      val cube3d = new Cube3D(lowPt3D, highPt3D)
      cubeList += cube3d
    })
    cubeList
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
    //新增一列TotalVolume计算总体积
    resultDataFrame = resultDataFrame.withColumn("TotalVolume", UCalculateVolume(col("RootNode.ChildNode.HighPt"), col("RootNode.ChildNode.LowPt")))
    resultDataFrame = resultDataFrame.withColumn("TotalJson", to_json(col("RootNode")))
    resultDataFrame.show()

    val redisConnect = new RedisConnector().GetRedisConnect()
    resultDataFrame = resultDataFrame.select(col("RootNode.ChildNode.HighPt"), col("RootNode.ChildNode.LowPt"), col("TotalVolume"), col("TotalJson"))
    resultDataFrame.show()
    //算最大交叠
    val dataFrame = resultDataFrame.collect()
    for (tableRow1 <- dataFrame) {
      val cubeList1 = GetCube3DList(tableRow1)
      for (tableRow2 <- dataFrame) {
        //同表循环2
        if (tableRow1 != tableRow2) {
          val cubeList2 = GetCube3DList(tableRow2)
          //开始计算最大交叠
          val totalPercent = VolumeAlgorithm.CalculatePercent(cubeList1, cubeList2, tableRow1.getAs[Double](2), tableRow2.getAs[Double](2))
          if (totalPercent > 0.8) {
            RedisInserter(tableRow1.getAs[String](3), redisConnect)
          }
        }
      }
    }
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
    sparkSession.stop()
  }
}

