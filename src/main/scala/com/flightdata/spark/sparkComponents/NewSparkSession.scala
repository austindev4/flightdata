package com.flightdata.spark.sparkComponents

import org.apache.spark.sql.SparkSession

trait NewSparkSession {

//  final def getSpark(): SparkSession = {

    lazy val spark: SparkSession = {
      SparkSession
      .builder()
      .appName("Total Monthly Flights")
      .master("local[*]")
      .getOrCreate()
    }

  //    sparkSession

//  }

}
