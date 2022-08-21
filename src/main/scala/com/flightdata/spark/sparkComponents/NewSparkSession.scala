package com.flightdata.spark.sparkComponents

import org.apache.spark.sql.SparkSession

object NewSparkSession {

  final def getSpark(): SparkSession = {

    val sparkSession = SparkSession
      .builder()
      .appName("Total Monthly Flights")
      .master("local[*]")
      .getOrCreate()

    sparkSession

  }

}
