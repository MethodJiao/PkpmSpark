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

    var BigX1: Int = 0;
    var LittleX1: Int = 0;
    var BigX2: Int = 0;
    var LittleX2: Int = 0;
    var BigY1: Int = 0;
    var LittleY1: Int = 0;
    var BigY2: Int = 0;
    var LittleY2: Int = 0;
    var BigZ1: Int = 0;
    var LittleZ1: Int = 0;
    var BigZ2: Int = 0;
    var LittleZ2: Int = 0;
    //X
    if (highPtOfCuboid1.x <= lowPtOfCuboid1.x) {
      BigX1 = lowPtOfCuboid1.x;
      LittleX1 = highPtOfCuboid1.x;
    }
    else {
      BigX1 = highPtOfCuboid1.x;
      LittleX1 = lowPtOfCuboid1.x
    }
    if (highPtOfCuboid2.x <= lowPtOfCuboid2.x) {
      BigX2 = lowPtOfCuboid2.x;
      LittleX2 = highPtOfCuboid2.x;
    }
    else {
      BigX2 = highPtOfCuboid2.x;
      LittleX2 = lowPtOfCuboid2.x;
    }

    //Y
    if (highPtOfCuboid1.y <= lowPtOfCuboid1.y) {
      BigY1 = lowPtOfCuboid1.y;
      LittleY1 = highPtOfCuboid1.y;
    }
    else {
      BigY1 = highPtOfCuboid1.y;
      LittleY1 = lowPtOfCuboid1.y;
    }
    if (highPtOfCuboid2.y <= lowPtOfCuboid2.y) {
      BigY2 = lowPtOfCuboid2.y;
      LittleY2 = highPtOfCuboid2.y;
    }
    else {
      BigY2 = highPtOfCuboid2.y;
      LittleY2 = lowPtOfCuboid2.y;
    }

    //Z
    if (highPtOfCuboid1.z <= lowPtOfCuboid1.z) {
      BigZ1 = lowPtOfCuboid1.z;
      LittleZ1 = highPtOfCuboid1.z;
    }
    else {
      BigZ1 = highPtOfCuboid1.z;
      LittleZ1 = lowPtOfCuboid1.z;
    }
    if (highPtOfCuboid2.z <= lowPtOfCuboid2.z) {
      BigZ2 = lowPtOfCuboid2.z;
      LittleZ1 = highPtOfCuboid2.z;
    }
    else {
      BigZ2 = highPtOfCuboid2.z;
      LittleZ2 = lowPtOfCuboid2.z;
    }

    if (BigX1 <= LittleX2 || BigX2 <= LittleX1)
      return 0
    else if (BigY1 <= LittleY2 || BigY2 <= LittleY1)
      return 0
    else if (BigZ1 <= LittleZ2 || BigZ2 <= LittleZ1)
      return 0

    val minX: Int = math.max(LittleX1, LittleX2)
    val minY: Int = math.max(LittleY1, LittleY2)
    val minZ: Int = math.max(LittleZ1, LittleZ2)
    val maxX: Int = math.min(BigX1, BigX2)
    val maxY: Int = math.min(BigY1, BigY2)
    val maxZ: Int = math.min(BigZ1, BigZ2)

    val ret: Double = ((maxX - minX) / 1000.0) * ((maxY - minY) / 1000.0) * ((maxZ - minZ) / 1000.0)
    return ret;
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

  //获取单条数据内的立方体合集
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

  //交叠最大百分比
  def CalculatePercent(cubeList1: ArrayBuffer[Cube3D], cubeList2: ArrayBuffer[Cube3D], cubeVolume1: Double, cubeVolume2: Double): Double = {
    var cube1MiniX, cube1MaxX = cubeList1.head.high.x
    var cube1MiniY, cube1MaxY = cubeList1.head.high.y
    var cube1MiniZ, cube1MaxZ = cubeList1.head.high.z

    var cube2MiniX, cube2MaxX = cubeList2.head.high.x
    var cube2MiniY, cube2MaxY = cubeList2.head.high.y
    var cube2MiniZ, cube2MaxZ = cubeList2.head.high.z

    cubeList1.foreach(cube3d => {
      if (cube3d.high.x < cube1MiniX) {
        cube1MiniX = cube3d.high.x
      }
      else if (cube3d.low.x < cube1MiniX) {
        cube1MiniX = cube3d.low.x
      }
      if (cube3d.high.x > cube1MaxX) {
        cube1MaxX = cube3d.high.x
      }
      else if (cube3d.low.x > cube1MaxX) {
        cube1MaxX = cube3d.low.x
      }

      if (cube3d.high.y < cube1MiniY) {
        cube1MiniY = cube3d.high.y
      }
      else if (cube3d.low.y < cube1MiniY) {
        cube1MiniY = cube3d.low.y
      }
      if (cube3d.high.y > cube1MaxY) {
        cube1MaxY = cube3d.high.y
      }
      else if (cube3d.low.y > cube1MaxY) {
        cube1MaxY = cube3d.low.y
      }

      if (cube3d.high.z < cube1MiniZ) {
        cube1MiniZ = cube3d.high.z
      }
      else if (cube3d.low.z < cube1MiniZ) {
        cube1MiniZ = cube3d.low.z
      }
      if (cube3d.high.z > cube1MaxZ) {
        cube1MaxZ = cube3d.high.z
      }
      else if (cube3d.low.z > cube1MaxZ) {
        cube1MaxZ = cube3d.low.z
      }

    })

    cubeList2.foreach(cube3d => {
      if (cube3d.high.x < cube2MiniX) {
        cube2MiniX = cube3d.high.x
      }
      else if (cube3d.low.x < cube2MiniX) {
        cube2MiniX = cube3d.low.x
      }
      if (cube3d.high.x > cube2MaxX) {
        cube2MaxX = cube3d.high.x
      }
      else if (cube3d.low.x > cube2MaxX) {
        cube2MaxX = cube3d.low.x
      }

      if (cube3d.high.y < cube2MiniY) {
        cube2MiniY = cube3d.high.y
      }
      else if (cube3d.low.y < cube2MiniY) {
        cube2MiniY = cube3d.low.y
      }
      if (cube3d.high.y > cube2MaxY) {
        cube2MaxY = cube3d.high.y
      }
      else if (cube3d.low.y > cube2MaxY) {
        cube2MaxY = cube3d.low.y
      }

      if (cube3d.high.z < cube2MiniZ) {
        cube2MiniZ = cube3d.high.z
      }
      else if (cube3d.low.z < cube2MiniZ) {
        cube2MiniZ = cube3d.low.z
      }
      if (cube3d.high.z > cube2MaxZ) {
        cube2MaxZ = cube3d.high.z
      }
      else if (cube3d.low.z > cube2MaxZ) {
        cube2MaxZ = cube3d.low.z
      }

    })
    //x要移动距离
    val distanceX = math.abs(cube2MaxX - cube2MiniX) + math.abs(cube1MaxX - cube1MiniX)
    val distanceY = math.abs(cube2MaxY - cube2MiniY) + math.abs(cube1MaxY - cube1MiniY)
    val distanceZ = math.abs(cube2MaxZ - cube2MiniZ) + math.abs(cube1MaxZ - cube1MiniZ)
    //初始位置设定
    val offsetX = cube1MiniX - cube2MaxX
    val offsetY = cube1MiniY - cube2MaxY
    val offsetZ = cube1MiniZ - cube2MaxZ
    cubeList2.foreach(cube3D => {
      cube3D.low.x += offsetX
      cube3D.high.x += offsetX
      cube3D.low.y += offsetY
      cube3D.high.y += offsetY
      cube3D.low.z += offsetZ
      cube3D.high.z += offsetZ
    })
    var maxTotalPercent = 0.0
    //开始三维对比
    //x维度
    for (xValue <- 0 to(distanceX, 100)) {
      //y维度
      for (yValue <- 0 to(distanceY, 100)) {
        //z维度
        for (zValue <- 0 to(distanceZ, 100)) {
          if ((xValue == 100) && (yValue == 6700) && (zValue == 100)) {
            print(1)
          }
          val totalPercent = ForEach3D(cubeList1, cubeList2, xValue, yValue, zValue, cubeVolume1, cubeVolume2)
          //val totalPercent = ForEach3D(cubeList1, cubeList2, 6700, 6700, 3400, cubeVolume1, cubeVolume2)
          if (totalPercent > maxTotalPercent) {
            maxTotalPercent = totalPercent
          }
        }
      }
    }
    maxTotalPercent
  }

  def ForEach3D(cubeList1: ArrayBuffer[Cube3D], cubeList2: ArrayBuffer[Cube3D], xValue: Int, yValue: Int, zValue: Int, cubeVolume1: Double, cubeVolume2: Double): Double = {
    val tempCubeList2 = new ArrayBuffer[Cube3D]
    cubeList2.foreach(cube => {
      tempCubeList2 += cube.clone().asInstanceOf[Cube3D]
    })

    var overlapTotalVolume = 0.0
    tempCubeList2.foreach(cube2 => {
      //val cube3D = new Cube3D(cube2.low.,cube2.high)
      cube2.high.x += xValue
      cube2.high.y += yValue
      cube2.high.z += zValue
      cube2.low.x += xValue
      cube2.low.y += yValue
      cube2.low.z += zValue
      cubeList1.foreach(cube1 => {
        val overlapVolume = OverlappingVolume(cube1.high, cube1.low, cube2.high, cube2.low)
        overlapTotalVolume += overlapVolume
      })
    })
    val cubeList1Percent = overlapTotalVolume / cubeVolume1
    val cubeList2Percent = overlapTotalVolume / cubeVolume2
    val totalPercent = math.sqrt(cubeList1Percent * cubeList2Percent)
    totalPercent
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

