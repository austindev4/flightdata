package com.flightdata.spark

import com.flightdata.spark.Applications.{flightTogetherRange, frequentFlyers, totalMonthlyFlights}
import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Datasets.{Flight, Passenger}
import com.flightdata.spark.sparkComponents.NewTestSparkSession
import com.github.mrpowers.spark.fast.tests.SchemaComparer
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FunSpecLike, FunSuiteLike}

class FunctionalTests extends NewTestSparkSession
  with FunSuiteLike{

  val testFlightData: Dataset[Flight] = flightDataLoader.flightds(spark)
  val testPassengerData: Dataset[Passenger] = flightDataLoader.passengersds(spark)

  test("Output row count of : total monthly flights analysis : is valid") {
    val actualDF: DataFrame = totalMonthlyFlights.result(spark, testFlightData)
    val actualCount = actualDF.count()

    val expectedCount = 12

    assert(actualCount === expectedCount)
  }

  test("Output schema of : total monthly flights : is valid") {
    val actualDF: DataFrame = totalMonthlyFlights.result(spark, testFlightData)
    val actualSchema = actualDF.schema

    val expectedSchema = StructType(Array(
      StructField("Month", IntegerType, false),
      StructField("Number_of_Flights", IntegerType, false)
    ))

    SchemaComparer.equals(expectedSchema, actualSchema)
  }

  test("Output of : top 100 frequent flyers : is valid") {
    val actualDF: DataFrame = frequentFlyers.result(spark,testFlightData,testPassengerData)
    val actualSchema = actualDF.schema

    val expectedSchema = StructType(Array(
      StructField("Passenger_ID", IntegerType, false),
      StructField("Number_of_Flights", IntegerType, false),
      StructField("First_Name", StringType, false),
      StructField("Last_Name", StringType, false),
    ))

    SchemaComparer.equals(expectedSchema, actualSchema)
  }

  test("Output of : passengers together on 3 flights : is valid") {
    val actualDF: DataFrame = flightTogetherRange.result(spark, testFlightData, testPassengerData)
    val actualSchema = actualDF.schema

    val expectedSchema = StructType(Array(
      StructField("Passenger_1_ID", IntegerType, false),
      StructField("Passenger_2_ID", IntegerType, false),
      StructField("Number_of_Flights_together", IntegerType, false),
      StructField("From", StringType, false),
      StructField("To", StringType, false),
    ))
    SchemaComparer.equals(expectedSchema, actualSchema)
  }

  test("Output of : passengers together on N flights in date range : is valid") {
    val actualDF: DataFrame = flightTogetherRange.result(spark, testFlightData, testPassengerData,
                      atLeastNTimes = 12, from = "2017-01-02 00:00:00", to = "2017-05-30 00:00:00")
    val actualSchema = actualDF.schema

    val expectedSchema = StructType(Array(
      StructField("Passenger_1_ID", IntegerType, false),
      StructField("Passenger_2_ID", IntegerType, false),
      StructField("Number_of_Flights_together", IntegerType, false),
      StructField("From", StringType, false),
      StructField("To", StringType, false),
    ))
    SchemaComparer.equals(expectedSchema, actualSchema)
  }

}
