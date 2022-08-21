package com.flightdata.spark.Applications

import com.flightdata.spark.Datasets.Flight
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.DataFrame

object totalMonthlyFlights {

  def result(ds: Dataset[Flight])(implicit spark: SparkSession): DataFrame = {

    ds.createOrReplaceTempView("FlightData")

    val totalMonthlyFlightsQuery = "select count(*), MONTH (date) as month from flightData group by month"
    spark.sql(totalMonthlyFlightsQuery)

  }

}
