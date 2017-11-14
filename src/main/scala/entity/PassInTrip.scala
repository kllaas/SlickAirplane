package entity

import java.sql.Timestamp
import java.util.Date

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Future

case class PassInTrip(tripNo: Int, date: Timestamp, idPsg: Int, place: String)

class PassInTripTable(tag: Tag) extends Table[PassInTrip](tag, "pass_in_trip") {
  val tripNo = column[Int]("trip_no", O.PrimaryKey, O.AutoInc)
  val date = column[Timestamp]("date")
  val idPsg = column[Int]("id_psg")
  val place = column[String]("place")

  def * = (tripNo, date, idPsg, place) <> (PassInTrip.apply _ tupled, PassInTrip.unapply)
}

object PassInTripTable {
  val table = TableQuery[PassInTripTable]
}

class PassInTripRepository(db: Database) {

  def create(passIntRip: PassInTrip): Future[PassInTrip] =
    db.run(PassInTripTable.table returning PassInTripTable.table += passIntRip)
}
