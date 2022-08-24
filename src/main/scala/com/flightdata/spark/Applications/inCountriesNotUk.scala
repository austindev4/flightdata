//package com.flightdata.spark.Applications
//
//import com.flightdata.spark.Datasets._
//import org.apache.spark.sql.{Dataset, SparkSession}
//import org.apache.spark.sql.DataFrame
//
////attempt this with map
//
//object inCountriesNotUk {
//
//  def longestRun(sq: Seq[String]): Int = {
//    sq.mkString(" ")
//      .split("UK")
//      .filter(_.nonEmpty)
//      .map(_.trim)
//      .map(s => s.split(" ").length)
//      .max
//  }
//
//
//  def result(ds1: Dataset[Flight], ds2: Dataset[Passenger])(implicit spark: SparkSession): DataFrame = {
//
//    ds1.createOrReplaceTempView("FlightData")
//    ds2.createOrReplaceTempView("Passengers")
//
////    trying to consolidate to list with as index. looks like exclusive to pyspark
////    ds1.groupBy("id", "to", as_index=false).count().show()
//
//    ds1.groupBy("passengerId").select("to").map(f=>f.getString(0)).collect.toList.show()
//
//  }
////    val data = List(
////    (1, List("UK", "IR", "AT", "UK", "CH", "PK")),
////    (2, List("CG", "IR")),
////    (3, List("CG", "IR", "SG", "BE", "UK")),
////    (4, List("CG", "IR", "NO", "UK", "SG", "UK", "IR", "TJ", "AT")),
////    (5, List("CG", "IR"))
////  )
//
//}
////
////  def result(flights: DataFrame, passengers: DataFrame): Unit = {
////    import org.graphframes._
////    val passengerset = passengers.distinct().cache()
////    val flightset = flights.cache()
////    val flightGraph = GraphFrame(passengerset, flightset)
////    val topTrips = flightGraph.edges.groupBy(“from",”to”).where(col(“to”) !== “UK”).select(“passengerId”, col(“*”) ).show()
////  }
////}

