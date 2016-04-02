package edu.kaist.tmtk.db

import java.io.Closeable

import com.datastax.driver.core.{Cluster, SimpleStatement, SocketOptions}
import edu.kaist.tmtk.{AsValue, log}
import resource.managed

import scala.collection.JavaConversions.{asScalaIterator, iterableAsScalaIterable, mapAsJavaMap, seqAsJavaList, setAsJavaSet}
import scala.collection.immutable.ListMap
import scala.collection.{Map, Seq, Set}

class Cassandra(path: String, var table: String = null, var schema: String = null, var order: String = null, lv: AnyRef = "I") extends Closeable {
  private val Array(host, name) = path.split("/")
  val connection = Cluster.builder.addContactPoint(host).withSocketOptions(new SocketOptions().setConnectTimeoutMillis(180000).setReadTimeoutMillis(600000)).build
  val session = connection.connect()
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $name WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
  session.execute(s"USE $name")
  if (table != null && schema != null)
    create(schema, order)
  override val toString = s"Cassandra($host/$name/${table.asStr("")})"
  log(s"[DONE] Connect $this", lv)

  override def close() = connection.close()

  def manage() = managed(this)

  def create(schema: String, order: String = null, table: String = null) = {
    if (table != null)
      this.table = table
    this.schema = schema
    this.order = order
    if (order == null)
      session.execute(s"CREATE TABLE IF NOT EXISTS ${this.table}($schema)")
    else
      session.execute(s"CREATE TABLE IF NOT EXISTS ${this.table}($schema) WITH CLUSTERING ORDER BY ($order)")
  }

  def insert(row: Map[String, Any], table: String = null) = {
    if (table != null)
      this.table = table
    val query = s"INSERT INTO ${this.table}(${row.keys.mkString(", ")}) VALUES (${Array.fill(row.size)("?").mkString(", ")})"
    session.execute(new SimpleStatement(query, row.values.map(asJava).map(_.asInstanceOf[AnyRef]).toArray: _*))
  }

  private def asJava(x: Any) = x match {
    case x: Seq[_] => seqAsJavaList(x)
    case x: Set[_] => setAsJavaSet(x)
    case x: Map[_, _] => mapAsJavaMap(x)
    case _ => x
  }

  def update(query: String, args: Any*) =
    session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))

  def size = count(this.table)

  def count(query: String, args: Any*) =
    session.execute(new SimpleStatement("SELECT COUNT(*) FROM " + query, args.map(_.asInstanceOf[AnyRef]): _*)).one.getLong(0)

  def one(query: String, args: Any*) = {
    val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
    Option(r).map(_.getObject(0)).orNull
  }

  def ones(query: String, args: Any*) =
    session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).map(x => x.getObject(0)).toList

  def row(query: String, args: Any*) = {
    val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
    Option(r).map(r => r.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)).orNull
  }

  def rows(query: String, args: Any*) = {
    val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
    for (r <- rs.iterator) yield rs.getColumnDefinitions.toSeq.indices.toList.map(r.getObject)
  }

  def map(query: String, args: Any*) = {
    val r = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*)).one
    Option(r).map(r => ListMap(r.getColumnDefinitions.map(_.getName).map(k => k -> r.getObject(k)).toSeq: _*)).orNull
  }

  def maps(query: String, args: Any*) = {
    val rs = session.execute(new SimpleStatement(query, args.map(_.asInstanceOf[AnyRef]): _*))
    for (r <- rs.iterator) yield ListMap(r.getColumnDefinitions.map(_.getName).map(k => k -> r.getObject(k)).toSeq: _*)
  }
}
