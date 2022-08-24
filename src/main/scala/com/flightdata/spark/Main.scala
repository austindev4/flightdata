package com.flightdata.spark

import com.flightdata.spark.sparkComponents.NewSparkSession
import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Applications._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  val logger = LoggerFactory.getLogger(getClass)
  implicit var sparkSession: SparkSession = _

  def main(args: Array[String]) {
    
    try {
      //create singleton spark session
      sparkSession = NewSparkSession.getSpark()


      // Load tables into Datasets
      val latestPassengerData = flightDataLoader.passengersds()
      val latestFlightData = flightDataLoader.flightds()


      // Total number of flights for each month
      totalMonthlyFlights.result(latestFlightData)
        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/totalMonthlyFlights.csv")
      //        .show(truncate = false)
      //        .collect().foreach(println)


      // Names of the 100 most frequent flyers.
      frequentFlyers.result(latestFlightData,latestPassengerData)
        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/frequentFlyers.csv")
      //        .show(truncate = false)
      //        .collect().foreach(println)


      // Passengers who have been on more than 3 flights together
      // flightTogether.result(latestFlightData, latestPassengerData)
        flightTogetherRange.result(latestFlightData, latestPassengerData)
        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/flightTogether.csv")
      //        .show(truncate = false)
      //        .collect().foreach(println)


      // Passengers who have been on more than N flights together
      flightTogetherRange.result(latestFlightData, latestPassengerData, atLeastNTimes=10, from="2017-01-02 00:00:00", to="2017-05-30 00:00:00")
        .write.mode(saveMode = "Overwrite").option("header", true).csv("data/flightTogetherRange.csv")
//        .show(truncate = false)
//        .collect().foreach(println)


    } catch {
      case e: Exception => {
        logger.error("Exception occurred while processing the data", e)
        throw e
      }
    } finally {
      logger.info(s"-::-  sparkSession.stop()")
      sparkSession.stop()
    }

    //Todo test case for schema validation
    //Todo pretty print result table - done
    //Todo cleanup logs

  }
}