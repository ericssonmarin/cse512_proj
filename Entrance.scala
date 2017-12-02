package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Entrance extends App {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  override def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("CSE512-HotspotAnalysis-ZERODAY") // YOU NEED TO CHANGE YOUR GROUP NAME
      .config("spark.some.config.option", "some-value").master("local[*]")
      .getOrCreate()

    //spark.sparkContext.getConf.set("spark.driver.memory", "8g")
    //spark.sparkContext.getConf.set("spark.executor.memory", "8g")

    paramsParser(spark, args)

    //print("Process Finished!!!")

  }

  private def paramsParser(spark: SparkSession, args: Array[String]): Unit = {
    var paramOffset = 1
    var currentQueryParams = ""
    var currentQueryName = ""
    var currentQueryIdx = -1

    //val args = Array("test/output","hotzoneanalysis","src/resources/point-hotzone.csv","src/resources/zone-hotzone.csv","hotcellanalysis",
    //  "src/resources/yellow_tripdata_2009-01_point.csv")

    //val args = Array("test/output","hotzoneanalysis","src/resources/point-hotzone.csv","src/resources/zone-hotzone.csv")
    //val args = Array("test/output","hotcellanalysis","src/resources/yellow_tripdata_2009-01_point.csv")
    val args = Array("test/output","hotcellanalysis","src/resources/yellow_tripdata_2009-01_point_1000.csv")

    while (paramOffset <= args.length) {
      if (paramOffset == args.length || args(paramOffset).toLowerCase.contains("analysis")) {
        // Turn in the previous query
        if (currentQueryIdx != -1) queryLoader(spark, currentQueryName, currentQueryParams, args(0) + currentQueryIdx)

        // Start a new query call
        if (paramOffset == args.length) return

        currentQueryName = args(paramOffset)
        currentQueryParams = ""
        currentQueryIdx = currentQueryIdx + 1
      }
      else {
        // Keep appending query parameters
        currentQueryParams = currentQueryParams + args(paramOffset) + " "
      }
      paramOffset = paramOffset + 1
    }
  }

  private def queryLoader(spark: SparkSession, queryName: String, queryParams: String, outputPath: String) {
    val queryParam = queryParams.split(" ")
    if (queryName.equalsIgnoreCase("hotcellanalysis")) {
      if (queryParam.length != 1) throw new ArrayIndexOutOfBoundsException("[CSE512] Query " + queryName + " needs 1 parameters but you entered " + queryParam.length)
      HotcellAnalysis.runHotcellAnalysis(spark, queryParam(0)).limit(50).repartition(1).write.mode(SaveMode.Overwrite).csv(outputPath)
    }
    else if (queryName.equalsIgnoreCase("hotzoneanalysis")) {
      if (queryParam.length != 2) throw new ArrayIndexOutOfBoundsException("[CSE512] Query " + queryName + " needs 2 parameters but you entered " + queryParam.length)
      HotzoneAnalysis.runHotZoneAnalysis(spark, queryParam(0), queryParam(1)).repartition(1).write.mode(SaveMode.Overwrite).csv(outputPath)
    }
    else {
      throw new NoSuchElementException("[CSE512] The given query name " + queryName + " is wrong. Please check your input.")
    }
  }
}
