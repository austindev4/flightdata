/**
 * INTRODUCTION
 * This Spark application calculates statistics from Flight data (100,000 Records) and Passenger data (15,500 Records).
 *
 * DIRECTORY STRUCTURE
 * flightdata/
 * ├─ data/
 * ├─ src/
 * │  ├─ main/scala/com/flightdata/spark/
 * │  ├─ test/scala/com/flightdata/spark/
 * ├─ project/
 *
 * INSTRUCTIONS - INPUT and OUTPUT
 * 1. Input data is csv format and to be copied to the data directory.
 * 2. Output provides 3 options, default is print to console (.show), uncomment+comment relevant commands
 *    to change output methods (.foreach(println)) or (.write.mode.csv).
 *
 * INSTRUCTIONS - EXECUTION
 * 1. Execute test cases by running test files located at src/test/scala/com/flightdata/spark/.
 *    Sample log of successful Test result is saved at root.
 *    3 Test suites includes 11 test cases :-
 *    a. DataQualityTests.scala
 *    b. FunctionalTests.scala
 *    c. UnitTests.scala
 * 2. Execute Main.scala (this file) to run the application. Else use spark-submit :
 *    To use spark-submit (C:\spark\spark-3.2.2-bin-hadoop3.2\bin\spark-submit.cmd
 *              --class com.flightdata.spark.Main C:\project\flightdata\target\scala-2.12\FlightData-assembly-1.0.jar)
 *    Ensure data directory containing csv's are co-located with the jar.
 *
 */

package com.flightdata.spark

import com.flightdata.spark.sparkComponents.NewSparkSession
import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Applications._
import org.slf4j.LoggerFactory

object Main extends NewSparkSession {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    try {
      // Load tables into Datasets
      val latestPassengerData = flightDataLoader.passengersds(spark)
      val latestFlightData = flightDataLoader.flightds(spark)

      // Total number of flights for each month
      totalMonthlyFlights.result(spark, latestFlightData)
  //        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/totalMonthlyFlights.csv")
          .show(truncate = false)
      //        .collect().foreach(println)


      // Names of the 100 most frequent flyers.
      frequentFlyers.result(spark, latestFlightData,latestPassengerData)
  //        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/frequentFlyers.csv")
          .show(truncate = false)
      //        .collect().foreach(println)


      // Longest run - incomplete
  //      longestRun.result(spark, latestFlightData)


      // Passengers who have been on more than 3 flights together
        flightTogetherRange.result(spark,latestFlightData, latestPassengerData)
  //        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/flightTogether.csv")
          .show(truncate = false)
      //        .collect().foreach(println)


      // Passengers who have been on more than N flights together
        flightTogetherRange.result(spark, latestFlightData, latestPassengerData, atLeastNTimes=10, from="2017-01-02 00:00:00", to="2017-05-30 00:00:00")
  //        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/flightTogetherRange.csv")
          .show(truncate = false)
  //        .collect().foreach(println)


    } catch {
      case e: Exception => {
        logger.error("Exception occurred while processing", e)
        throw e
      }
    } finally {
      logger.info(s"-::-  sparkSession.stop()")
      spark.stop()
    }

  }
}