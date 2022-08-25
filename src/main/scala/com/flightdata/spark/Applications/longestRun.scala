/**
INCOMPLETE SOLUTION

package com.flightdata.spark.Applications

import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Datasets.Flight
import com.flightdata.spark.sparkComponents.NewSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_list, expr}
import org.apache.spark.sql.functions._

object longestRun{

  def result(spark: SparkSession, latestFlightData: Dataset[Flight]): Unit ={

    //
    /**
     * SQL logic
     * Aggregate by the 'to' column, and group by passengerId
     */
    latestFlightData.createOrReplaceTempView("FlightData")
    val aggDataDF = spark.sql( """
              SELECT
                  passengerId,
                  aggregate(array_sort(collect_list(to)), '', (acc, x) -> concat(acc, x)) col3
              FROM FlightData
              GROUP BY passengerId
                  """)

  }
}

*/