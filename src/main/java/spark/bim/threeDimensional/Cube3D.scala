package spark.bim.threeDimensional

class Cube3D(lowPt: DPoint3D, highPt: DPoint3D) {
  var low: DPoint3D = lowPt
  var high: DPoint3D = highPt

  def rotation(rx: Int, ry: Int): Unit = {
    val lowTempX = (low.y - ry) + rx;
    val lowTempY = -(low.x - rx) + ry
    low.x = lowTempX
    low.y = lowTempY

    val highTempX = (high.y - ry) + rx;
    val highTempY = -(high.x - rx) + ry
    high.x = highTempX
    high.y = highTempY
  }
}
