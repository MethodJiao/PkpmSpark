package spark.bim

import com.mongodb.spark._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis
import spark.bim.threeDimensional.{Cube3D, DPoint3D, VolumeAlgorithm}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SparkMainTask {

  //Mongo数据来源
  def MongodbSearcher(sparkSession: SparkSession): DataFrame = {

    val dataFrame = MongoSpark.load(sparkSession)
    dataFrame.createOrReplaceTempView("netflows")
    var resultDataFrame = sparkSession.sql("select RootNode.ChildNode.HighPt,RootNode.ChildNode.LowPt from netflows")
    resultDataFrame = resultDataFrame.distinct()
    //resultDataFrame.show()
    resultDataFrame
  }

  //长方体求相交体积
  def OverlappingVolume(highPtOfCuboid1: DPoint3D, lowPtOfCuboid1: DPoint3D, highPtOfCuboid2: DPoint3D, lowPtOfCuboid2: DPoint3D): Double = {

    if (highPtOfCuboid1.z <= lowPtOfCuboid2.z || highPtOfCuboid2.z <= lowPtOfCuboid1.z)
      return 0
    else if (highPtOfCuboid1.x <= lowPtOfCuboid2.x || highPtOfCuboid2.x <= lowPtOfCuboid1.x)
      return 0
    else if (highPtOfCuboid1.y <= lowPtOfCuboid2.y || highPtOfCuboid2.y <= lowPtOfCuboid1.y)
      return 0

    val minX: Int = math.max(lowPtOfCuboid1.x, lowPtOfCuboid2.x)
    val minY: Int = math.max(lowPtOfCuboid1.y, lowPtOfCuboid2.y)
    val minZ: Int = math.max(lowPtOfCuboid1.z, lowPtOfCuboid2.z)
    val maxX: Int = math.min(highPtOfCuboid1.x, highPtOfCuboid2.x)
    val maxY: Int = math.min(highPtOfCuboid1.y, highPtOfCuboid2.y)
    val maxZ: Int = math.min(highPtOfCuboid1.z, highPtOfCuboid2.z)

    ((maxX - minX) / 1000) * ((maxY - minY) / 1000) * ((maxZ - minZ) / 1000)
  }

  //写入redis
  def RedisInserter(name: String, redisConnect: Jedis): Unit = {
    redisConnect.lpush("name", name)
  }

  //UDF计算总体积
  def UdfCalculateVolume(highPts: mutable.WrappedArray[GenericRow], lowPts: mutable.WrappedArray[GenericRow]): Double = {
    val lowAndHighZip = highPts.zip(lowPts)
    var totalVolume = 0.0
    for (lowAndHigh <- lowAndHighZip) {
      val highPt = lowAndHigh._1.toSeq
      val lowPt = lowAndHigh._2.toSeq
      val highPt3D: DPoint3D = new DPoint3D(highPt.apply(0).##, highPt.apply(1).##, highPt.apply(2).##)
      val lowPt3D = new DPoint3D(lowPt.apply(0).##, lowPt.apply(1).##, lowPt.apply(2).##)
      totalVolume += VolumeAlgorithm.OrthogonalVolume(highPt3D, lowPt3D)
    }
    totalVolume
  }

  def GetCube3DList(row: Row): ArrayBuffer[Cube3D] = {
    val highPts: mutable.WrappedArray[GenericRow] = row.getAs[mutable.WrappedArray[GenericRow]](0)
    val lowPts: mutable.WrappedArray[GenericRow] = row.getAs[mutable.WrappedArray[GenericRow]](1)
    val lowHighZip = highPts.zip(lowPts)
    val cubeList = ArrayBuffer.empty[Cube3D] //单条数据内所有的cube
    lowHighZip.foreach(lowHigh => {
      val highPt = lowHigh._1.toSeq
      val lowPt = lowHigh._2.toSeq
      val highPt3D: DPoint3D = new DPoint3D(highPt.apply(0).##, highPt.apply(1).##, highPt.apply(2).##)
      val lowPt3D = new DPoint3D(lowPt.apply(0).##, lowPt.apply(1).##, lowPt.apply(2).##)
      val cube3d = new Cube3D(lowPt3D, highPt3D)
      cubeList += cube3d
    })
    cubeList
  }

  //UDF交叠最大百分比
  def CalculatePercent(cubeList1: ArrayBuffer[Cube3D], cubeList2: ArrayBuffer[Cube3D], cubeVolume1: Double, cubeVolume2: Double): Double = {
    print(cubeVolume1)
    cubeVolume1
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
    resultDataFrame = resultDataFrame.withColumn("TotalVolume", UCalculateVolume(col("HighPt"), col("LowPt")))
    resultDataFrame.show()

    //算最大交叠
    val dataFrame = resultDataFrame.collect()
    for (tableRow1 <- dataFrame) {
      val cubeList1 = GetCube3DList(tableRow1)
      for (tableRow2 <- dataFrame) {
        //同表循环2
        if (tableRow1 != tableRow2) {
          val cubeList2 = GetCube3DList(tableRow2)
          //这里开始计算最大交叠
          CalculatePercent(cubeList1, cubeList2, tableRow1.getAs[Double](2), tableRow2.getAs[Double](2))
          print(cubeList1)
          print(cubeList2)
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

