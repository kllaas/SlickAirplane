package entity

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Future


case class Passenger(idPsg: Int, name: String)

class PassengerTable(tag: Tag) extends Table[Passenger](tag, "passengers") {
  val idPsg = column[Int]("id_psg", O.PrimaryKey, O.AutoInc)
  val name = column[String]("name")

  def * = (idPsg, name) <> (Passenger.apply _ tupled, Passenger.unapply)
}

object PassengerTable {
  val table = TableQuery[PassengerTable]
}

class PassengerRepository(db: Database) {
  def create(country: Passenger): Future[Passenger] =
    db.run(PassengerTable.table returning PassengerTable.table += country)
}
