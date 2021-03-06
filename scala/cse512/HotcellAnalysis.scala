package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection._

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

    val cells_full = pickupInfo.collect() //grabing all existing cells

    println("Creating the distinct list of cells")

    val cells_1 = cells_full.distinct //grabing all existing cells

    println("Number of Cells" + cells_1.length.toString)

    println("Creating the second distinct list of cells")

    val cells_2 = cells_1.clone() //making a copy of the distinct cells

    var point_1 = new Array[Int](2)
    var point_2 = new Array[Int](2)
    var feature: Long = 0
    var accumulatedFeature: Long = 0
    var accumulatedWeights: Long = 0
    var x_bar: Double = 0.0
    var x_square: Double = 0.0
    var feature_sum_values: Double = 0.0
    var s: Double = 0.0
    var g_numerador: Double = 0.0
    var g_denominator: Double = 0.0
    var getis_ord: Double = 0.0
    var features = scala.collection.mutable.Map[(String, String, String), Long]()
    var lstData : List[(Int,Int,Int,Double)] = List()
    var lstDataFinal : List[(Int,Int,Int)] = List()

    println("Calculating the features")

    var i: Integer = 0

    // Calculating here the global metrics "x_bar" and "s"

    for (cell <- cells_full) {

      i += 1

      println("iteration: " + i.toString + " out of " + cells_full.length.toString)

      if (features.contains((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString))) {
        features((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString)) += 1
      } else {
        features += ((cell.get(0).toString, cell.get(1).toString, cell.get(2).toString) -> 1)
      }

    }

    //println(features.values)

    x_bar = features.values.sum / numCells

    features.keys.foreach{
      a => x_square += math.pow(features(a),2)
    }

    s = math.sqrt((x_square / numCells) - math.pow(x_bar, 2))

    // Calculating here the getis_ord for each cell

    println("Iterating over the cells")

    i = 0

    for (cell_1 <- cells_1) {

      i += 1

      println("iteration: " + i.toString + " out of " + cells_1.length.toString)

      accumulatedFeature = 0
      accumulatedWeights = 0
      g_numerador = 0
      g_denominator = 0
      getis_ord = 0

      for (cell_2 <- cells_2) {

        // The next four test conditions verify if the cell is a neighbor of the analyzed cell

        //if (cell_1.toString != cell_2.toString) { no need this, since the exactly same cell should be included in the calculations

          if (cell_1.get(2).toString.toInt == (cell_2.get(2).toString.toInt - 1) || cell_1.get(2).toString.toInt == cell_2.get(2).toString.toInt || cell_1.get(2).toString.toInt == (cell_2.get(2).toString.toInt + 1)) {

            if (cell_1.get(0).toString.toInt == (cell_2.get(0).toString.toInt - 1) || cell_1.get(0).toString.toInt == cell_2.get(0).toString.toInt || cell_1.get(0).toString.toInt == (cell_2.get(0).toString.toInt + 1)) {

              if (cell_1.get(1).toString.toInt == (cell_2.get(1).toString.toInt - 1) || cell_1.get(1).toString.toInt == cell_2.get(1).toString.toInt || cell_1.get(1).toString.toInt == (cell_2.get(1).toString.toInt + 1)) {

                feature = features((cell_2.get(0).toString, cell_2.get(1).toString, cell_2.get(2).toString))

                accumulatedFeature += feature
                accumulatedWeights += 1

              //} else {

              //  println("longitude fails!")

              }

            //} else {

            //  println("latitude fails!")

            }

          //} else {

          //  println("date fails!")

          }

        //} else {

        //  println("same cell!")

        //}

      }

      if (accumulatedFeature > 0) {

        g_numerador = accumulatedFeature - (accumulatedWeights * x_bar)
        g_denominator = math.sqrt(((numCells * accumulatedWeights) - math.pow(accumulatedWeights, 2)) / (numCells - 1))
        getis_ord = g_numerador/(s * g_denominator)

        // Storing the getis_ord caculated in a data frame

        lstData = lstData:+((cell_1.get(0).toString.toInt,cell_1.get(1).toString.toInt,cell_1.get(2).toString.toInt,getis_ord.toString.toDouble))

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
