package edu.kaist.tmtk

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.Map

package object db {
  def main(args: Array[String]) {
    args.at(0, null) match {
      case "MySQL" => testMySQL()
      case "Cassandra" => testCassandra()
      case _ =>
    }
  }

  def testMySQL() = test(method, () => {
    for (db <- new MySQL("143.248.48.105/unit", "admin", "admin1", "samples", "i int, name varchar(20), primary key(i)").manage()) {
      warn(s" + [DB] $db")
      db.update("DELETE FROM samples WHERE i<=?", 1000)
      for (r <- Map("i" -> 1, "name" -> "A") :: Map("i" -> 2, "name" -> "B") :: Map("i" -> 3, "name" -> "C") :: Nil)
        db.insert(r)
      db.update("UPDATE samples SET name=? WHERE i=?", "CC", 3)

      val a0 = db.size
      warn(s"   - size : $a0")
      val a1 = db.one("SELECT name FROM samples WHERE i=?", 1)
      warn(s"   -  one : $a1")
      val a2 = db.ones("SELECT name FROM samples WHERE i>=?", 2)
      warn(s"   - ones : ${a2.mkString(", ")}")
      val List(i, n) = db.row("SELECT * FROM samples WHERE i=?", 1)
      warn(s"   -  row : $i->$n")
      val a4 = for (List(i, n) <- db.rows("SELECT * FROM samples WHERE i>=?", 2)) yield s"$i->$n"
      warn(s"   - rows : ${a4.mkString(", ")}")
      val a5 = db.map("SELECT * FROM samples WHERE i=?", 1)
      warn(s"   -  map : $a5")
      val a6 = for (r <- db.maps("SELECT * FROM samples WHERE i>=?", 2)) yield r
      warn(s"   - maps : ${a6.mkString(", ")}")
    }
  })

  def testCassandra() = test(method, () => {
    for (db <- new Cassandra("143.248.48.105/unit", "samples", "v int, i int, name text, alias list<text>, primary key(v, i)").manage()) {
      warn(s" + [DB] $db")
      db.update("DELETE from samples WHERE v=0 AND i<=?", 1000)
      for (r <- Map("v" -> 0, "i" -> 1, "name" -> "A", "alias" -> List("A", "a")) :: Map("v" -> 0, "i" -> 2, "name" -> "B", "alias" -> List("B", "b")) :: Map("v" -> 0, "i" -> 3, "name" -> "C", "alias" -> List("C", "c")) :: Nil)
        db.insert(r)
      db.update("UPDATE samples SET name=? WHERE v=0 AND i=?", "CC", 3)

      val a0 = db.size
      warn(s"   - size : $a0")
      val a1 = db.one("SELECT name FROM samples WHERE v=0 AND i=?", 1)
      warn(s"   -  one : $a1")
      val a2 = db.ones("SELECT name FROM samples WHERE v=0 AND i>=?", 2)
      warn(s"   - ones : ${a2.mkString(", ")}")
      val List(i, n, a) = db.row("SELECT i, name, alias FROM samples WHERE v=0 AND i=?", 1)
      warn(s"   -  row : $i->($n, $a)")
      val a4 = for (List(i, n, a) <- db.rows("SELECT i, name, alias FROM samples WHERE v=0 AND i>=?", 2)) yield s"$i->($n, $a)"
      warn(s"   - rows : ${a4.mkString(", ")}")
      val a5 = db.map("SELECT i, name, alias FROM samples WHERE v=0 AND i=?", 1)
      warn(s"   -  map : $a5")
      val a6 = for (r <- db.maps("SELECT i, name, alias FROM samples WHERE v=0 AND i>=?", 2)) yield r
      warn(s"   - maps : ${a6.mkString(", ")}")
    }
  })
}
