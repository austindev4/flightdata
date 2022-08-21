package com.flightdata.spark.Applications

import com.flightdata.spark.Datasets._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.DataFrame

object frequentFlyers {

  def result(ds1: Dataset[Flight], ds2: Dataset[Passenger])(implicit spark: SparkSession): DataFrame = {

    ds1.createOrReplaceTempView("FlightData")
    ds2.createOrReplaceTempView("PassengerData")

    val frequentFlyers = """
      select
          f.passengerId,
          count(*) as Number_of_Flights
      from FlightData as f
      left join PassengerData as p
      on f.passengerId = p.passengerId
      group by f.passengerId
      order by Number_of_Flights desc
      limit 100
      """
    spark.sql(frequentFlyers)

  }

}




