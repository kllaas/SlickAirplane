package entity


import java.sql.Timestamp
import java.util.Date

import slick.jdbc.PostgresProfile.api._
import slick.lifted.Tag
import scala.concurrent.Future

case class Trip(tripNo: Int, idComp: Int, plane: String, townTo: String, townFrom: String, timeOut: Timestamp, timeIn: Timestamp)

class TripTable(tag: Tag) extends Table[Trip](tag, "trips") {
  val trip_no = column[Int]("trip_no", O.PrimaryKey)
  val id_comp = column[Int]("id_comp")
  val plane = column[String]("plane")
  val town_from = column[String]("town_from")
  val town_to = column[String]("town_to")
  val time_out = column[Timestamp]("time_out")
  val time_in = column[Timestamp]("time_in")

  val id_comp_fk = foreignKey("id_comp_fk", id_comp, TableQuery[CompanyTable])(_.id)

  def * = (trip_no, id_comp, plane, town_to, town_from, time_in, time_out) <> (Trip.apply _ tupled, Trip.unapply)
}

object TripTable {
  val table = TableQuery[TripTable]
}

class TripRepository(db: Database) {
  def create(trip: Trip): Future[Trip] = {
    db.run(TripTable.table returning TripTable.table += trip)
  }

}
