package com.flightdata.spark.DataLoaders

import com.flightdata.spark.Datasets._
import com.flightdata.spark.Main._

import org.apache.spark.sql._


object flightDataLoader {

  // Load flightData.csv into a Dataset
  def flightds()(implicit spark: SparkSession): Dataset[Flight] = {
    import spark.implicits._

    logger.info(s":::: Spark Session State :: " + spark.sessionState + ": :::")

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/flightData.csv")
      .as[Flight]

  }

  // Load passengers.csv into a dataset
  def passengersds()(implicit spark: SparkSession): Dataset[Passenger] = {
    import spark.implicits._

    logger.info(s":::: Spark Session State :: " + spark.sessionState + ": :::")

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/passengers.csv")
      .as[Passenger]

  }

}
