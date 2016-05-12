package edu.kaist.tmtk.db

import java.io.Closeable

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import edu.kaist.tmtk._
import org.bson.Document
import org.bson.conversions.Bson
import resource.managed

import scala.collection.JavaConversions._

class MongoDB(path: String, var table: String = null, reset: Boolean = false, lv: AnyRef = "I") extends Closeable {
  private val Array(host, name) = path.split("/", 2)
  val connection = new MongoClient(host)
  val database = connection.getDatabase(name)
  var session: MongoCollection[Document] = null
  if (table != null) {
    session = database.getCollection(table)
    if (reset)
      drop()
  }
  log(s"[DONE] Connect $this", lv)

  override def toString = s"MongoDB($host/$name/$table)"

  override def close() = connection.close()

  def manage() = managed(this)

  def set(table: String) =
    if (table != null) {
      this.table = table
      session = database.getCollection(table)
    }

  def drop(table: String = null) = {
    set(table)
    session.drop()
  }

  def insert(row: AMap[String, Any], table: String = null) = {
    set(table)
    session.insertOne(new Document(row.asJMap))
    row
  }

  def delete(query: Bson, table: String = null) = {
    set(table)
    session.deleteMany(query)
  }

  def update(query: Bson, oper: Bson, table: String = null) = {
    set(table)
    session.updateOne(query, oper)
  }

  def replace(query: Bson, row: AMap[String, Any], table: String = null) = {
    set(table)
    session.replaceOne(query, new Document(row.asJMap))
  }

  def size: Long = size()

  def size(table: String = null): Long = {
    set(table)
    session.count()
  }

  def count(query: Bson, table: String = null) = {
    set(table)
    session.count(query)
  }

  def one(field: String, query: Bson, table: String = null) = {
    set(table)
    Option(map(query)).map(_ (field)).orNull
  }

  def ones(field: String, query: Bson, table: String = null) = {
    set(table)
    maps(query).map(_ (field))
  }

  def row(fields: String, query: Bson, table: String = null) = {
    set(table)
    val r = map(query)
    fields.split(",").toList.map(_.trim).map(r)
  }

  def rows(fields: String, query: Bson, table: String = null) = {
    set(table)
    maps(query).map(fields.split(",").toList.map(_.trim).map(_))
  }

  def map(query: Bson, table: String = null) = {
    set(table)
    session.find(query).first()
  }

  def maps(query: Bson, table: String = null) = {
    set(table)
    session.find(query).toStream
  }
}
