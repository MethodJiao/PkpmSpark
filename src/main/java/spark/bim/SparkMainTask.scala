package spark.bim

import com.alibaba.fastjson.JSON
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
      hashCode %= 2353639
    }
    hashCode
  }

  def main(args: Array[String]): Unit = {
    //mongo spark通信槽
    val sparkSession = SparkSession.builder()
      .appName("PKPMBimAnalyse")
      .config("spark.mongodb.input.uri", "mongodb://10.100.140.127/mydb.netflows")
      .master("local")
      .getOrCreate()

    //UDF注册
    val UCalculateVolume = udf(UdfCalculateVolume _)
    var resultDataFrame = MongodbSearcher(sparkSession)
    //新增列TotalVolume计算总体积 TotalJson整行json数据 编Guid
    resultDataFrame = resultDataFrame.withColumn("TotalVolume", UCalculateVolume(col("RootNode.ChildNode.HighPt"), col("RootNode.ChildNode.LowPt")))
    resultDataFrame = resultDataFrame.withColumn("TotalJson", to_json(col("RootNode")))
    resultDataFrame = resultDataFrame.select(col("RootNode.ChildNode.HighPt"), col("RootNode.ChildNode.LowPt"), col("TotalVolume"), col("TotalJson"))
    resultDataFrame.show()

    //清除历史数据
    //RedisConnector.getInstance.GetRedisConnect().flushAll()
    //初始行数
    val rowCount = resultDataFrame.count() - 1
    //求笛卡尔积
    resultDataFrame = resultDataFrame.join(resultDataFrame, resultDataFrame("TotalVolume") =!= resultDataFrame("TotalVolume"), "inner")
    //削减笛卡尔分区
    resultDataFrame = resultDataFrame.coalesce(200)
    //权重值
    var weightValue = 0
    var forNumCount: Long = 0
    resultDataFrame.foreach(row => {
      val cubeList1 = VolumeAlgorithm.GetCube3DList(row.getAs[mutable.WrappedArray[GenericRow]](0), row.getAs[mutable.WrappedArray[GenericRow]](1))
      val cubeList2 = VolumeAlgorithm.GetCube3DList(row.getAs[mutable.WrappedArray[GenericRow]](4), row.getAs[mutable.WrappedArray[GenericRow]](5))
      val totalPercent = VolumeAlgorithm.CalculatePercent(cubeList1, cubeList2, row.getAs[Double](2), row.getAs[Double](6))
      if (totalPercent > 0.8) {
        //权重累加
        weightValue += 1
      }
      //推荐时机计数
      forNumCount += 1
      if (forNumCount == rowCount) {
        //redis推荐记录
        if (weightValue > 0) {
          val totalJson = row.getAs[String](3)
          val jsonObject = JSON.parseObject(totalJson)
          jsonObject.put("WeightValue", weightValue)
          val key = jsonObject.getString("KeyValue")
          jsonObject.remove("KeyValue")
          //插入推荐数据
          val redisConnect = RedisConnector.getInstance.GetRedisConnect()
          redisConnect.lpush(key, jsonObject.toString)

        }
        weightValue = 0
        forNumCount = 0
      }
    })
    resultDataFrame.show()
    RedisConnector.getInstance.GetRedisConnect().close()
    sparkSession.stop()
  }
}

