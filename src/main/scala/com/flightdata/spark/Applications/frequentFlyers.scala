package com.flightdata.spark.Applications

import com.flightdata.spark.Datasets._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.DataFrame

object frequentFlyers {

  /**
   * The greatest number of countries a passenger has been in without being in the UK.
   * @param ds1
   * @param ds2
   * @param spark
   * @return
   */
  def result(spark: SparkSession, ds1: Dataset[Flight], ds2: Dataset[Passenger]): DataFrame = {

    ds1.createOrReplaceTempView("FlightData")
    ds2.createOrReplaceTempView("Passengers")

       /**
        -- SET UP CTE
        -- - select top 100 Passenger_ID and its count as Number_of_Flights
        -- - group by Passenger_ID and order by Number_of_Flights
        -- CREATE
        -- final query using CTE
        */
    val frequentFlyers = """
                            WITH freq AS (
                              SELECT
                                f.passengerId AS Passenger_ID,
                                count("passengerId") as Number_of_Flights
                              FROM flightData AS f
                              GROUP BY f.passengerId
                              ORDER BY Number_of_Flights DESC
                              LIMIT 100
                            )
                            SELECT Passenger_ID, Number_of_Flights, p.firstName AS First_Name, p.lastName AS Last_Name
                            FROM freq
                            INNER JOIN passengers AS p
                            ON p.passengerId = freq.Passenger_ID;
      """

    spark.sql(frequentFlyers)

  }

}




