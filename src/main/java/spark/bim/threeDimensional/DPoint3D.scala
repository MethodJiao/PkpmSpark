package spark.bim.threeDimensional

//三维点类
class DPoint3D(mx: Int, my: Int, mz: Int) {
  var x: Int = mx
  var y: Int = my
  var z: Int = mz

  //逆90度
  def rotation(rx: Int, ry: Int): Unit = {
    val tempX = -(y - ry) + rx;
    val tempY = (x - rx) + ry
    x = tempX
    y = tempY
  }
}
