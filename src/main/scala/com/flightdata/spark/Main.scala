package com.flightdata.spark

import com.flightdata.spark.sparkComponents.NewSparkSession
import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Applications._

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

object Main {

  val logger = LoggerFactory.getLogger(getClass)
  implicit var sparkSession: SparkSession = _

  def main(args: Array[String]) {
    
    try {
      //create spark session
      sparkSession = NewSparkSession.getSpark()

      // Total number of flights for each month
      val latestFlightData = flightDataLoader.flightds()
      totalMonthlyFlights.result(latestFlightData).sort("month").collect().foreach(println)

      // Names of the 100 most frequent flyers.
      val frequentFlyersData = flightDataLoader.passengersds()
      frequentFlyers.result(latestFlightData,frequentFlyersData).collect().foreach(println)

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

  }
}