package spark.bim.threeDimensional

//三维点类
class DPoint3D(mx: Int, my: Int, mz: Int) {
  var x: Int = mx
  var y: Int = my
  var z: Int = mz

  //依据极坐标圆方程化简式
  //x` = x * cos(α) - y * sin(α)
  //y` = y * cos(α) + x * sin(α)
  //顺90度
  def rotation(rx: Int, ry: Int): Unit = {
    val tempX = (y - ry) + rx;
    val tempY = -(x - rx) + ry
    x = tempX
    y = tempY
  }

  def distance(pt: DPoint3D): Double = {
    math.sqrt(math.pow((x - pt.x), 2) + math.pow((y - pt.y), 2) + math.pow((z - pt.z), 2))
  }
}
