package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    var rectangle:Array[String] = new Array[String](4)
    rectangle = queryRectangle.split(",")
    val rectX1 = rectangle(0).trim().toDouble
    val rectY1 = rectangle(1).trim().toDouble
    val rectX2 = rectangle(2).trim().toDouble
    val rectY2 = rectangle(3).trim().toDouble

    var point:Array[String] = new Array[String](2)
    point = pointString.split(",")
    val pX = point(0).trim().toDouble
    val pY = point(1).trim().toDouble

    val xMin = math.min(rectX1, rectX2)
    val xMax = math.max(rectX1, rectX2)
    val yMin = math.min(rectY1, rectY2)
    val yMax = math.max(rectY1, rectY2)

    if (xMin<=pX && pX<=xMax && yMin<=pY && pY<=yMax)
      return true
    return false
  }

}
