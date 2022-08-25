package com.flightdata.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.sparkComponents.NewTestSparkSession
import com.flightdata.spark.Datasets.{Flight, Passenger}

import org.scalatest.FunSuiteLike
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, SchemaComparer}


class DataQualityTests extends NewTestSparkSession
  with FunSuiteLike
  with DataFrameComparer {

  val testFlightData: Dataset[Flight] = flightDataLoader.flightds(spark)
  val testPassengerData: Dataset[Passenger] = flightDataLoader.passengersds(spark)


  test("Location codes in 'to' columns are valid") {

    import spark.implicits._
    val selectColTo = testFlightData.select("to").map(f => f.getString(0))
      .dropDuplicates
      .collect.toList

    val predefinedToList = List("ir", "cn", "tj", "pk", "no", "us", "sg", "nl",
      "cl", "il", "uk", "cg", "be", "bm", "at", "co", "th", "ch", "dk", "ca", "tk", "au", "iq", "fr", "jo", "ar", "se")
    selectColTo should contain theSameElementsAs predefinedToList
  }


  test("Location codes in 'from' columns are valid") {

    import spark.implicits._
    val selectColFrom = testFlightData.select("from").map(f => f.getString(0))
      .dropDuplicates
      .collect.toList

    val predefinedFromList = List("cg", "co", "ca", "ch", "tk", "be", "at", "nl",
       "dk", "tj", "us", "fr", "jo", "bm", "ar", "th", "sg", "ir", "au", "uk", "pk", "il", "cl", "iq", "se", "cn", "no")
    selectColFrom should contain theSameElementsAs predefinedFromList
  }


  test("Input schema for flightData source is valid") {

    val expectedFlightSchema = ScalaReflection.schemaFor[Flight].dataType.asInstanceOf[StructType]
    val actualSchema = testFlightData.schema

    SchemaComparer.equals(expectedFlightSchema, actualSchema)
  }

  test("Input schema for passengerData source is valid") {

    val expectedPassengerSchema = ScalaReflection.schemaFor[Passenger].dataType.asInstanceOf[StructType]
    val actualSchema = testPassengerData.schema

    SchemaComparer.equals(expectedPassengerSchema, actualSchema)
  }

}


