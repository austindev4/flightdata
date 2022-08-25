package com.flightdata.spark.Applications

import com.flightdata.spark.Datasets.{Flight, Passenger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object flightTogether {

  /**
   * Passengers who have been on more than 3 flights together
   * @param ds1
   * @param ds2
   * @param spark
   * @return
   */

  def result(ds1: Dataset[Flight], ds2: Dataset[Passenger])(implicit spark: SparkSession): DataFrame = {

    ds1.createOrReplaceTempView("FlightData")
    ds2.createOrReplaceTempView("Passengers")

    /**
     * Document SQL logic
     * Inner join on flightData table on Flight ID, Date and
     * Passenger ID where Passenger ID is not equal.
     * Group by Passenger 1 & 2 ID's and filter the result
     * on Count > 3.
     * Output select Passenger 1 ID, Passenger 2 ID, Count as
     * Number_of_Flights_together, min and max date.
     */
    val flightTogether =
      """
                          SELECT
                            f1.passengerId AS Passenger_1_ID,
                            f2.passengerId AS Passenger_2_ID,
                            COUNT(*) AS Number_of_Flights_together,
                            MIN(f1.date) AS From,
                            MAX(f2.date) AS To
                          FROM flightData AS f1
                          INNER JOIN flightData AS f2
                          ON f1.passengerId != f2.passengerId and
                            f1.flightId = f2.flightId and
                            f1.date = f2.date
                          GROUP BY f1.passengerId, f2.passengerId
                          HAVING Number_of_Flights_together >= 3
      """

    spark.sql(flightTogether)

  }
}
