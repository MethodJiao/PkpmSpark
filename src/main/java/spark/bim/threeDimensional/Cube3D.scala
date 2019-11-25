package spark.bim.threeDimensional

class Cube3D(lowPt: DPoint3D, highPt: DPoint3D) {
  val low: DPoint3D = lowPt
  val high: DPoint3D = highPt

  def rotation(rx: Int, ry: Int, angle: Double): Unit = {
    low.rotation(rx, ry, angle)
    high.rotation(rx, ry, angle)
  }

  override def clone(): AnyRef = {
    new Cube3D(this.low.clone().asInstanceOf[DPoint3D], this.high.clone().asInstanceOf[DPoint3D])
  }
}
