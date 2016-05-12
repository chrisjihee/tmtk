package edu.kaist.tmtk.db

import java.io.Closeable
import java.sql.DriverManager

import edu.kaist.tmtk._
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers._
import resource.managed

import scala.collection.JavaConversions._

class HSQLDB(path: String, var table: String = null, var schema: String = null, user: String = null, pswd: String = null, reset: Boolean = false, lv: AnyRef = "I") extends Closeable {
  val connection = DriverManager.getConnection(s"jdbc:hsqldb:$path", user, pswd)
  val session = connection.createStatement()
  if (table != null && schema != null)
    create(schema)
  lazy val runner = new QueryRunner
  log(s"[DONE] Connect $this", lv)

  override def toString = s"HSQLDB($path/${table.asStr("")})"

  override def close() = connection.close()

  def manage() = managed(this)

  def create(schema: String, table: String = null) = {
    if (table != null)
      this.table = table
    this.schema = schema
    if (reset)
      session.execute(s"DROP TABLE IF EXISTS ${this.table}")
    session.execute(s"CREATE TABLE IF NOT EXISTS ${this.table}($schema)")
  }

  def insert(row: AMap[String, Any], table: String = null) = {
    if (table != null)
      this.table = table
    val query = s"INSERT INTO ${this.table}(${row.keys.mkString(", ")}) VALUES (${Array.fill(row.size)("?").mkString(", ")})"
    runner.update(connection, query, row.values.map(_.asInstanceOf[AnyRef]).toArray: _*)
  }

  def update(query: String, args: Any*) =
    runner.update(connection, query, args.map(_.asInstanceOf[AnyRef]): _*)

  def size = count(this.table)

  def count(query: String, args: Any*) =
    runner.query(connection, "SELECT COUNT(*) FROM " + query, new ScalarHandler[Long], args.map(_.asInstanceOf[AnyRef]): _*)

  def one(query: String, args: Any*) =
    runner.query(connection, query, new ScalarHandler[AnyRef], args.map(_.asInstanceOf[AnyRef]): _*)

  def ones(query: String, args: Any*) =
    runner.query(connection, query, new ColumnListHandler[AnyRef], args.map(_.asInstanceOf[AnyRef]): _*)

  def row(query: String, args: Any*) =
    runner.query(connection, query, new ArrayHandler, args.map(_.asInstanceOf[AnyRef]): _*).toList

  def rows(query: String, args: Any*) =
    runner.query(connection, query, new ArrayListHandler, args.map(_.asInstanceOf[AnyRef]): _*).toStream.map(_.toList)

  def map(query: String, args: Any*) =
    runner.query(connection, query, new MapHandler, args.map(_.asInstanceOf[AnyRef]): _*)

  def maps(query: String, args: Any*) =
    runner.query(connection, query, new MapListHandler, args.map(_.asInstanceOf[AnyRef]): _*)
}
