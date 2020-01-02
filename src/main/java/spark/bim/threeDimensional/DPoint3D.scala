package spark.bim.threeDimensional

//三维点类
class DPoint3D(mx: Int, my: Int, mz: Int) {
  var x: Int = mx
  var y: Int = my
  var z: Int = mz

  //依据极坐标圆方程化简式
  //x` = x * cos(α) - y * sin(α)
  //y` = y * cos(α) + x * sin(α)
  //旋转
  def rotation(rx: Int, ry: Int, angle: Double): Unit = {
    val tempX = ((x - rx) * math.cos(angle)) - ((y - ry) * math.sin(angle))
    val tempY = ((y - ry) * math.cos(angle)) + ((x - rx) * math.sin(angle))
    x = tempX.toInt + rx
    y = tempY.toInt + ry
  }

  def distance(pt: DPoint3D): Double = {
    math.sqrt(math.pow(x - pt.x, 2) + math.pow(y - pt.y, 2) + math.pow(z - pt.z, 2))
  }

  override def clone(): AnyRef = {
    new DPoint3D(this.x, this.y, this.z)
  }
}
