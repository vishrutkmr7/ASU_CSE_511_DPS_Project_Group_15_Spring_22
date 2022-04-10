package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

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
    else
      return false
  }

   def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    var point1:Array[String] = new Array[String](2)
    point1 = pointString1.split(",")
    val point1X1 = point1(0).trim().toDouble
    val point1Y1 = point1(1).trim().toDouble

    var point2:Array[String] = new Array[String](2)
    point2 = pointString2.split(",")
    val point2X2 = point2(0).trim().toDouble
    val point2Y2 = point2(1).trim().toDouble

    val euclid_dist = math.pow(math.pow((point1X1 - point2X2), 2) + math.pow((point1Y1 - point2Y2), 2),0.5)

    if (euclid_dist > distance)
      return false
    else
      return true
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(((ST_Contains(queryRectangle, pointString)))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
