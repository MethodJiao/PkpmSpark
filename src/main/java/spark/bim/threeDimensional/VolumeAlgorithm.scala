package spark.bim.threeDimensional

object VolumeAlgorithm {

  def Abs(num: Int): Int = {
    if (num > 0) num else -num
  }

  //正交体积
  def OrthogonalVolume(pt1: DPoint3D, pt2: DPoint3D): Double = {
    val length = Abs(pt1.x - pt2.x) / 1000.0
    val width = Abs(pt1.y - pt2.y) / 1000.0
    val height = Abs(pt1.z - pt2.z) / 1000.0
    length * width * height
  }
}
