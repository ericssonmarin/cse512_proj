package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.collection._
import util.control.Breaks._

object HotcellAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {

    import spark.implicits._

    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
    spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
    spark.udf.register("CalculateZ", (pickupTime: String) => HotcellUtils.CalculateCoordinate(pickupTime, 2))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")

    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // converting DF to RDD
    var pickupInfoRdd : RDD[Row] = pickupInfo.rdd


    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    //The implementation starts here

    println("Creating the full list of cells")

    //val cells_full = pickupInfo.collect() //grabing all existing cells

    println("Creating the distinct list of cells")

    var feature: Long = 0
    var accumulatedFeature: Long = 0
    var accumulatedWeights: Long = 0
    var x_bar: Double = 0.0
    var x_square: Double = 0.0
    var s: Double = 0.0
    var g_numerador: Double = 0.0
    var g_denominator: Double = 0.0
    var getis_ord: Double = 0.0
    var features = scala.collection.mutable.Map[(String, String, String), Long]()
    var lstData : List[(Int,Int,Int,Double)] = List()
    var lstDataFinal : List[(Int,Int,Int)] = List()
    var neighbor: Integer = 0

    println("Calculating the features")

    //var i: Integer = 0

    // Calculating here the global metrics "x_bar" and "s"

    for (cell <- pickupInfoRdd.collect()) {


      if (features.contains((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString))) {
        features((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString)) += 1
      } else {
        features += ((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString) -> 1)
      }

    }

    //println(features.values)

    x_bar = pickupInfo.count() / numCells

    features.keys.foreach{
      a => x_square += math.pow(features(a),2)
    }

    s = math.sqrt((x_square / numCells) - math.pow(x_bar, 2))

    // Calculating here the getis_ord for each cell

    println("Iterating over the cells")

    for (cell_1 <- features.keys) {

      accumulatedFeature = 0
      accumulatedWeights = 0
      g_numerador = 0
      g_denominator = 0
      getis_ord = 0

      neighbor = 0

      breakable{
      for (cell_2 <- features.keys) {

        // The next four test conditions verify if the cell is a neighbor of the analyzed cell

        if (cell_1._3.toInt == (cell_2._3.toInt - 1) || cell_1._3.toInt == cell_2._3.toInt || cell_1._3.toInt == (cell_2._3.toInt + 1)) {

          if (cell_1._1.toInt == (cell_2._1.toInt - 1) || cell_1._1.toInt == cell_2._1.toInt || cell_1._1.toInt == (cell_2._1.toInt + 1)) {

            if (cell_1._2.toInt == (cell_2._2.toInt - 1) || cell_1._2.toInt == cell_2._2.toInt || cell_1._2.toInt == (cell_2._2.toInt + 1)) {

              feature = features((cell_2._1, cell_2._2, cell_2._3))

              accumulatedFeature += feature
              accumulatedWeights += 1

              neighbor += 1

              if (neighbor == 27) {
                break
              }

            }

          }

        }

      }

      }

      if (accumulatedFeature > 0) {

        g_numerador = accumulatedFeature - (accumulatedWeights * x_bar)
        g_denominator = math.sqrt(((numCells * accumulatedWeights) - math.pow(accumulatedWeights, 2)) / (numCells - 1))
        getis_ord = g_numerador/(s * g_denominator)

        // Storing the getis_ord caculated in a data frame

        lstData = lstData:+((cell_1._1.toInt,cell_1._2.toInt,cell_1._3.toInt,getis_ord.toString.toDouble))

      }

    }

    println("Dumping the cells in a dataframe")

    var lstToDf = spark.sparkContext.parallelize(lstData).toDF("lat", "long", "date", "getis_ord")

    lstToDf.createOrReplaceTempView("records")
    lstToDf.show()

    lstToDf = spark.sql("select lat,long, date from records order by getis_ord desc")
    lstToDf.show()

    //return lstToDf.orderBy($"getis_ord".desc)

    return lstToDf

  }
}
