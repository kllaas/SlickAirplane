package entity

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Future

case class Company(id: Int, name: String)

class CompanyTable(tag: Tag) extends Table[Company](tag, "companies") {
  val id = column[Int]("id", O.PrimaryKey)
  val name = column[String]("name")

  def * = (id, name) <> (Company.apply _ tupled, Company.unapply)
}

object CompanyTable{
  val table = TableQuery[CompanyTable]
}

class CompanyRepository(db: Database) {
  def create(company: Company): Future[Company] =
    db.run(CompanyTable.table returning CompanyTable.table += company)
}
