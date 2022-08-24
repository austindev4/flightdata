package com.flightdata.spark.Applications

import com.flightdata.spark.Datasets.{Flight, Passenger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object flightTogetherRange {

  /**
   * Passengers who have been on more than N flights together within Date range
   * @param ds1
   * @param ds2
   * @param atLeastNTimes
   * @param from
   * @param to
   * @param spark
   * @return
   */
  def result(ds1: Dataset[Flight], ds2: Dataset[Passenger], atLeastNTimes: Int = 3, from: String = "1900-01-02 00:00:00", to: String = "2099-01-02 00:00:00")(implicit spark: SparkSession): DataFrame = {

    ds1.createOrReplaceTempView("FlightData")
    ds2.createOrReplaceTempView("Passengers")

    /**
     * Inner join on flightData table on Flight ID, Date and
     * Passenger ID where Passenger ID is not equal.
     * Group by Passenger 1 & 2 ID's and filter the result
     * on Count > 3.
     * Output select Passenger 1 ID, Passenger 2 ID, Count as
     * Number_of_Flights_together, min and max date.
     */
    val flightTogether =
      s"""
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
                          WHERE (f1.date) >= '$from'
                            AND (f2.date) <= '$to'
                          GROUP BY f1.passengerId, f2.passengerId
                          HAVING Number_of_Flights_together >= $atLeastNTimes
      """

    spark.sql(flightTogether)

  }
}
