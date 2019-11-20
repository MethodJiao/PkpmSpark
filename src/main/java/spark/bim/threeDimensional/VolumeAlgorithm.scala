package spark.bim.threeDimensional

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object VolumeAlgorithm {

  //正交体积
  def OrthogonalVolume(pt1: DPoint3D, pt2: DPoint3D): Double = {
    val length = math.abs(pt1.x - pt2.x) / 1000.0
    val width = math.abs(pt1.y - pt2.y) / 1000.0
    val height = math.abs(pt1.z - pt2.z) / 1000.0
    length * width * height
  }

  //长方体求相交体积
  def OverlappingVolume(highPtOfCuboid1: DPoint3D, lowPtOfCuboid1: DPoint3D, highPtOfCuboid2: DPoint3D, lowPtOfCuboid2: DPoint3D): Double = {

    var BigX1: Int = 0
    var LittleX1: Int = 0
    var BigX2: Int = 0
    var LittleX2: Int = 0
    var BigY1: Int = 0
    var LittleY1: Int = 0
    var BigY2: Int = 0
    var LittleY2: Int = 0
    var BigZ1: Int = 0
    var LittleZ1: Int = 0
    var BigZ2: Int = 0
    var LittleZ2: Int = 0
    //X
    if (highPtOfCuboid1.x <= lowPtOfCuboid1.x) {
      BigX1 = lowPtOfCuboid1.x
      LittleX1 = highPtOfCuboid1.x
    }
    else {
      BigX1 = highPtOfCuboid1.x
      LittleX1 = lowPtOfCuboid1.x
    }
    if (highPtOfCuboid2.x <= lowPtOfCuboid2.x) {
      BigX2 = lowPtOfCuboid2.x
      LittleX2 = highPtOfCuboid2.x
    }
    else {
      BigX2 = highPtOfCuboid2.x
      LittleX2 = lowPtOfCuboid2.x
    }

    //Y
    if (highPtOfCuboid1.y <= lowPtOfCuboid1.y) {
      BigY1 = lowPtOfCuboid1.y
      LittleY1 = highPtOfCuboid1.y
    }
    else {
      BigY1 = highPtOfCuboid1.y
      LittleY1 = lowPtOfCuboid1.y
    }
    if (highPtOfCuboid2.y <= lowPtOfCuboid2.y) {
      BigY2 = lowPtOfCuboid2.y
      LittleY2 = highPtOfCuboid2.y
    }
    else {
      BigY2 = highPtOfCuboid2.y
      LittleY2 = lowPtOfCuboid2.y
    }

    //Z
    if (highPtOfCuboid1.z <= lowPtOfCuboid1.z) {
      BigZ1 = lowPtOfCuboid1.z
      LittleZ1 = highPtOfCuboid1.z
    }
    else {
      BigZ1 = highPtOfCuboid1.z
      LittleZ1 = lowPtOfCuboid1.z
    }
    if (highPtOfCuboid2.z <= lowPtOfCuboid2.z) {
      BigZ2 = lowPtOfCuboid2.z
      LittleZ1 = highPtOfCuboid2.z
    }
    else {
      BigZ2 = highPtOfCuboid2.z
      LittleZ2 = lowPtOfCuboid2.z
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
    ret
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

  //三维空间遍历
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
}
