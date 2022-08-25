package com.flightdata.spark.sparkComponents

import org.apache.spark.sql.SparkSession

trait NewTestSparkSession {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("Testing Flights Data Application")
      .master("local[*]")
      .getOrCreate()
  }
}
