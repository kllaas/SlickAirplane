import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import data.HardCoredRepository
import entity._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import slick.jdbc.PostgresProfile.api._

import scala.collection.mutable

object Main {
  val db = Database.forURL(
    "jdbc:postgresql://127.0.0.1/airport?user=alex&password=alex"
  )

  val companiesRepository = new CompanyRepository(db)
  val tripRepository = new TripRepository(db)
  val passInTripRepository = new PassInTripRepository(db)
  val passengersRepository = new PassengerRepository(db)

  def main(args: Array[String]): Unit = {
    //        init()
    //        databaseFill()
    //    task63()
    //    task67()
    //    task72
    //    task77
    //    task79
    task84
  }

  def init(): Unit = {
    Await.result(db.run(CompanyTable.table.schema.create), Duration.Inf)
    Await.result(db.run(TripTable.table.schema.create), Duration.Inf)
    Await.result(db.run(PassengerTable.table.schema.create), Duration.Inf)
    Await.result(db.run(PassInTripTable.table.schema.create), Duration.Inf)
  }

  case class Test(a: Int, b: Int, c: String, d: Int)

  val tuple: (Int, Int, String, Int) = (1, 2, "a", 3)

  Test.tupled(tuple) // works

  def databaseFill(): Unit = {
    val companies = HardCoredRepository.companies
    val trips = HardCoredRepository.trips
    val passInTrip = HardCoredRepository.pass_in_trip
    val passengers = HardCoredRepository.passengers

    val dateFormat = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS")


    for (companyData <- companies) {
      Await.result(companiesRepository.create(Company.tupled(companyData)), Duration.Inf)
    }


    for (passData <- passengers) {
      Await.result(passengersRepository.create(Passenger.tupled(passData)), Duration.Inf)
    }


    for (tripData <- trips) {

      val dataFrom = new Timestamp(dateFormat.parse(tripData._6).getTime)
      val dataTo = new Timestamp(dateFormat.parse(tripData._7).getTime)

      val trip = Trip(tripData._1, tripData._2, tripData._3, tripData._4, tripData._5, dataFrom, dataTo)

      Await.result(tripRepository.create(trip), Duration.Inf)
    }

    for (passData <- passInTrip) {

      val dataTo = new Timestamp(dateFormat.parse(passData._2).getTime)
      val passInTrip = PassInTrip(passData._1, dataTo, passData._3, passData._4)

      Await.result(passInTripRepository.create(passInTrip), Duration.Inf)
    }

  }


  def task63() = {
    val query = (PassInTripTable.table join PassengerTable.table on (_.idPsg === _.idPsg))
      .groupBy { case (passInTrip, passengers) => (passInTrip.place, passengers.name) }
      .map { case (place, group) => (place._2, group.length, place._1) }
      .filter(a => a._2 > 1)
      .map(a => a._1 -> a._3)

    print(Await.result(db.run(query.result), Duration.Inf))
  }

  def task67() = {
    val query = TripTable.table.map(trip => (trip.town_from, trip.town_to))
      .groupBy(trip => (trip._1, trip._2))
      .map { tuple => tuple._1 -> tuple._2.length }

    val routes = Await.result(db.run(query.result), Duration.Inf)

    var maxFlights = 0

    for (route <- routes) {
      maxFlights = Math.max(maxFlights, route._2)
    }

    routes.filter(_._2 == maxFlights).foreach(println)
  }

  def task72() = {

    val query = ((TripTable.table join PassInTripTable.table on (_.trip_no === _.tripNo)) join PassengerTable.table on (_._2.idPsg === _.idPsg))
      .groupBy((t) => t._2.name -> t._1._1.id_comp)
      .map(x => x._1._1 -> x._1._2 -> x._2.length)
      .groupBy((t) => t._1._1 -> t._2)
      .map(t => t._1._1 -> t._2.length -> t._1._2)
      .filter(x => x._1._2 === 1)

    val routes = Await.result(db.run(query.result), Duration.Inf)

    var maxFlights = 0

    for (route <- routes) {
      maxFlights = Math.max(maxFlights, route._2)
    }

    routes.filter(_._2 == maxFlights).foreach(item => println(item._1._1 + " - " + item._2))
  }

  def task77() = {
    val query = (TripTable.table join PassInTripTable.table on (_.trip_no === _.tripNo))
      .filter {
        _._1.town_from === "Rostov"
      }
      .groupBy { filteredTable => (filteredTable._2.date, filteredTable._1.town_from) }
      .map { groupedByDate => (groupedByDate._1._1, groupedByDate._1._2, groupedByDate._2.length) }

    val list = Await.result(db.run(query.result), Duration.Inf)
    val max = list.maxBy(_._3)._3

    list.filter(_._3 == max).foreach(println)
  }

  def task79() = {

    val query = (TripTable.table join PassInTripTable.table on (_.trip_no === _.tripNo) join PassengerTable.table on (_._2.idPsg === _.idPsg))
      .groupBy { joinedTables => joinedTables._2.name -> joinedTables._1._1.time_out -> joinedTables._1._1.time_in }
      .map { groupedByName => (groupedByName._1._1._1, groupedByName._1._1._2, groupedByName._1._2) }

    val result = Await.result(db.run(query.result), Duration.Inf)

    val resultAsLists = result.map(t => mutable.MutableList(t._1, t._2.getTime, t._3.getTime))
    var max: Long = 0

    for (item <- resultAsLists) {
      if (item(1).asInstanceOf[Long] > item(2).asInstanceOf[Long]) {

        item(2) = item(2).asInstanceOf[Long] + 24 * 60 * 60 * 1000

      }

      max = Math.max(item(2).asInstanceOf[Long] - item(1).asInstanceOf[Long], max)
    }

    resultAsLists.filter {
      x =>
        val currDiff = x(2).asInstanceOf[Long] - x(1).asInstanceOf[Long]
        currDiff == max
    }.foreach {
      x =>
        println(x.head.asInstanceOf[String] + ": " + (x(2).asInstanceOf[Long] - x(1).asInstanceOf[Long]) / 1000 / 60)
    }
  }


  /*
      Find the names of the different passengers,
      which flew only by the same route (there and back or in the one direction).

  */
  def task84() = {
    val query = (TripTable.table join PassInTripTable.table on (_.trip_no === _.tripNo) join PassengerTable.table on (_._2.idPsg === _.idPsg))
      .groupBy {
        joinedTables => (joinedTables._2.name, joinedTables._1._1.town_from, joinedTables._1._1.town_to)
      }.map {
      groupedByName => (groupedByName._1._1, groupedByName._1._2, groupedByName._1._3)
    }

    val result = Await.result(db.run(query.result), Duration.Inf)

    result.foreach(x => println(x))

    val mapIds = new util.HashMap[String, (String, String)]()
    val setIdsToDelete = new mutable.HashSet[String]()

    for (i <- 1 until result.size) {
      val currTowns = (result(i)._2, result(i)._3)

      if (mapIds.containsKey(result(i)._1)) {

        val prevTowns = mapIds.get(result(i)._1)

        if (!((prevTowns._1 == currTowns._1) && (prevTowns._2 == currTowns._2)))
          if (!((prevTowns._1 == currTowns._2) && (prevTowns._2 == currTowns._1)))
            setIdsToDelete.add(result(i)._1)

      } else {
        mapIds.put(result(i)._1, currTowns)
      }
    }

    result.filter(x => !setIdsToDelete.contains(x._1))
      .groupBy(x => x._1)
      .foreach(x => println(x._1))
  }

  def task110 = {

  }

}
