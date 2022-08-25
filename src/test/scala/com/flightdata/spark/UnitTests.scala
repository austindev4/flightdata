package com.flightdata.spark

import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Datasets.{Flight, Passenger}
import com.flightdata.spark.sparkComponents.NewTestSparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.FunSpecLike
import org.scalatest.Matchers.{a, convertToAnyShouldWrapper, not, empty}

class UnitTests extends NewTestSparkSession
  with FunSpecLike{

  it("Data loader should work") {
    flightDataLoader.flightds(spark) shouldBe a [Dataset[_]]
  }

  it("Both datasource's should exist"){
    flightDataLoader.flightds(spark) should not be empty
    flightDataLoader.passengersds(spark) should not be empty
  }

}
