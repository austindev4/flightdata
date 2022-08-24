/**
 * Test case 1
 *
 */

package com.flightdata.spark

import com.flightdata.spark.Applications._
import com.flightdata.spark.DataLoaders.flightDataLoader
import com.flightdata.spark.Main._
import com.flightdata.spark.Datasets._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class Test extends AnyFunSuite
  with totalMonthlyFlights
  with BeforeAndAfterAll {

  var sparkTestSession : SparkSession = _

  override  def beforeAll(): Unit = {
//    val sparkTestSession = NewTestSparkSession.getSpark()
//    sparkTestSession = SparkSession.builder().appName("Test Total Monthly Flights").master("local[*]").getOrCreate()
    sparkTestSession =  SparkSession
          .builder()
          .appName("Test Total Monthly Flights")
          .master("local[*]")
          .getOrCreate()
  }

  override def afterAll(): Unit = {
    println("calling spark stop mate")
    sparkTestSession.stop()
  }

  test("Data loading as txt"){
    val spark = sparkTestSession
    import spark.implicits._
    val peopleDF = spark.read.textFile("data/passengers.csv").map(_.split(",")).toDF()
    peopleDF.printSchema()
  }

  test("Data loading as csv") {
    var spark = sparkTestSession
//    import spark.implicits._
    val testFlightData = flightDataLoader.flightds()
    val result = totalMonthlyFlights.result(testFlightData)
    result.printSchema()
  }

  test("Reading an invalid file location using readTextfileToDataSet should throw an exception") {
    intercept[Exception] {
      val spark = sparkTestSession
      import org.apache.spark.sql.functions.col
      val df = spark.read.textFile("mocklocation/fakefile.csv")
      df.show()
    }
  }


}
