package com.flightdata.spark.Applications

import com.flightdata.spark.Datasets.Flight
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.DataFrame

object totalMonthlyFlights {

  /**
   * The total number of flights for each month.
   * @param ds
   * @param spark
   * @return
   */
  def result(spark: SparkSession, ds: Dataset[Flight]): DataFrame = {
    ds.createOrReplaceTempView("FlightData")
    val totalMonthlyFlightsQuery =
      """
        SELECT
            MONTH (date) as Month,
            COUNT(*) as Number_of_Flights
        FROM flightData
        GROUP BY Month
        ORDER BY Month
        """
    spark.sql(totalMonthlyFlightsQuery)
  }

}
