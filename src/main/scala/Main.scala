import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import data.HardCoredRepository
import entity._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import slick.jdbc.PostgresProfile.api._

object Main {
  val db = Database.forURL(
    "jdbc:postgresql://127.0.0.1/airport?user=alex&password=alex"
  )

  val companiesRepository = new CompanyRepository(db)
  val tripRepository = new TripRepository(db)
  val passInTripRepository = new PassInTripRepository(db)
  val passengersRepository = new PassengerRepository(db)

  def main(args: Array[String]): Unit = {
    //    init()
    //    databaseFill()
    task63()
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


    for (tripData <- trips) {

      val dataFrom = new Timestamp(dateFormat.parse(tripData._6).getTime)
      val dataTo = new Timestamp(dateFormat.parse(tripData._7).getTime)

      val trip = Trip(tripData._1, tripData._2, tripData._3, tripData._4, tripData._5, dataFrom, dataTo)

      Await.result(tripRepository.create(trip), Duration.Inf)
    }

    for (passData <- passengers) {
      Await.result(passengersRepository.create(Passenger.tupled(passData)), Duration.Inf)
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
      .map { case (place, group) => (place._2,group.length, place._1)}
      .filter( a => a._2 > 1)
      .map ( a => a._1 -> a._3)

    print(Await.result(db.run(query.result), Duration.Inf))
  }

}
