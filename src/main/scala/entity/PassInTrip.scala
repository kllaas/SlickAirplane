package entity

import java.sql.Timestamp
import java.util.Date

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Future

case class PassInTrip(tripNo: Int, date: Timestamp, idPsg: Int, place: String)

class PassInTripTable(tag: Tag) extends Table[PassInTrip](tag, "pass_in_trip") {
  val tripNo = column[Int]("trip_no")
  val date = column[Timestamp]("date")
  val idPsg = column[Int]("id_psg")
  val place = column[String]("place", O.Length(10))

  val tripFk = foreignKey("trip_no_fk", tripNo, TableQuery[TripTable])(_.trip_no)
  val passFk = foreignKey("id_psg_fk", idPsg, TableQuery[PassengerTable])(_.idPsg)

  val passInTripPk = primaryKey("pit_pk", (tripNo, idPsg, date))

  def * = (tripNo, date, idPsg, place) <> (PassInTrip.apply _ tupled, PassInTrip.unapply)
}

object PassInTripTable {
  val table = TableQuery[PassInTripTable]
}

class PassInTripRepository(db: Database) {

  def create(passIntRip: PassInTrip): Future[PassInTrip] =
    db.run(PassInTripTable.table returning PassInTripTable.table += passIntRip)
}
