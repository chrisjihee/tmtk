package edu.kaist.tmtk.db

import java.io.Closeable
import java.sql.DriverManager

import edu.kaist.tmtk.{AsValue, log}
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers._

import scala.collection.Map

class MySQL(path: String, user: String, pswd: String, var table: String = null, var schema: String = null, lv: AnyRef = "I") extends Closeable {
  val connection = DriverManager.getConnection(s"jdbc:mysql://$path", user, pswd)
  val session = connection.createStatement()
  val runner = new QueryRunner()
  if (table != null && schema != null)
    create(schema)
  override val toString = s"MySQL($user@$path/${table.asStr("")})"
  log(s"[DONE] Connect $this", lv)

  override def close() = connection.close()

  def create(schema: String, table: String = null) = {
    if (table != null)
      this.table = table
    this.schema = schema
    session.execute(s"CREATE TABLE IF NOT EXISTS ${this.table}($schema)")
  }

  def insert(row: Map[String, Any], table: String = null) = {
    if (table != null)
      this.table = table
    val query = s"INSERT INTO ${this.table}(${row.keys.mkString(", ")}) VALUES (${Array.fill(row.size)("?").mkString(", ")})"
    runner.update(connection, query, row.values.map(_.asInstanceOf[AnyRef]).toArray: _*)
  }

  def update(query: String, args: Any*) =
    runner.update(connection, query, args.map(_.asInstanceOf[AnyRef]): _*)

  def count(query: String, args: Any*) =
    runner.query(connection, "SELECT COUNT(*) FROM " + query, new ScalarHandler[Long], args.map(_.asInstanceOf[AnyRef]): _*)

  def one(query: String, args: Any*) =
    runner.query(connection, query, new ScalarHandler[AnyRef], args.map(_.asInstanceOf[AnyRef]): _*)

  def ones(query: String, args: Any*) =
    runner.query(connection, query, new ColumnListHandler[AnyRef], args.map(_.asInstanceOf[AnyRef]): _*)

  def row(query: String, args: Any*) =
    runner.query(connection, query, new ArrayHandler, args.map(_.asInstanceOf[AnyRef]): _*)

  def rows(query: String, args: Any*) =
    runner.query(connection, query, new ArrayListHandler, args.map(_.asInstanceOf[AnyRef]): _*)

  def map(query: String, args: Any*) =
    runner.query(connection, query, new MapHandler, args.map(_.asInstanceOf[AnyRef]): _*)

  def maps(query: String, args: Any*) =
    runner.query(connection, query, new MapListHandler, args.map(_.asInstanceOf[AnyRef]): _*)
}