package com.flightdata.spark.Datasets

case class Flight(
                       passengerId: Int,
                       flightId: Int,
                       from:String,
                       to:String,
                       date:String
                     )

case class Passenger(
                   passengerId:Int,
                   firstName:String,
                   lastName:String,
                 )
