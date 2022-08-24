name := "FlightData"
version := "1.0"
organization := "com.flightdata"
scalaVersion := "2.12.16"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.2" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.12" % Test
)
