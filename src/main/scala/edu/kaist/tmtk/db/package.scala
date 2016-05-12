package edu.kaist.tmtk

import com.mongodb.client.model.{Filters, Updates}

import scala.collection.JavaConversions._

package object db {
  def main(args: Array[String]) {
    args.at(0, null) match {
      case "Xlsx" => testXlsx()
      case "H2" => testH2()
      case "HSQLDB" => testHSQLDB()
      case "SQLite" => testSQLite()
      case "MySQL" => testMySQL()
      case "Cassandra" => testCassandra()
      case "MongoDB" => testMongoDB()
      case "All" => testXlsx(); testH2(); testHSQLDB(); testSQLite(); testMySQL(); testCassandra(); testMongoDB()
      case _ => test("edu.kaist.tmtk.db")
    }
  }

  def testXlsx() = test(method, () => {
    for (db <- new Xlsx("/DB/test.xlsx", "samples", Seq("i", "name")).manage()) {
      warn(s" + [DB] $db")
      (1 to 1000).foreach(db.delete)
      for (r <- Map("i" -> 1, "name" -> "A") :: Map("i" -> 2, "name" -> "B") :: Map("i" -> 3, "name" -> "C") :: Nil)
        db.insert(r)
      db.update(3, Map("name" -> "CC"))
    }

    for (db <- new Xlsx("/DB/test.xlsx", "samples").manage()) {
      val a0 = db.size
      warn(s"   - size : $a0")
      val a1 = db.one(1, "name")
      warn(s"   -  one : $a1")
      val a2 = db.ones("name")
      warn(s"   - ones : ${a2.mkString(", ")}")
      val List(i, n) = db.row(1)
      warn(s"   -  row : ${i.asInt}->$n")
      val a4 = for (List(i, n) <- db.rows) yield s"${i.asInt}->$n"
      warn(s"   - rows : ${a4.mkString(", ")}")
      val a5 = db.map(1)
      warn(s"   -  map : $a5")
      val a6 = for (r <- db.maps) yield r
      warn(s"   - maps : ${a6.mkString(", ")}")
    }
  })

  def testH2() = test(method, () => {
    for (db <- new H2("/DB/unit", "samples", "i int, name varchar(20), primary key(i)").manage()) {
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

  def testHSQLDB() = test(method, () => {
    for (db <- new HSQLDB("/DB/unit", "samples", "i int, name varchar(20), primary key(i)").manage()) {
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

  def testSQLite() = test(method, () => {
    for (db <- new SQLite("/DB/unit", "samples", "i int, name varchar(20), primary key(i)").manage()) {
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

  def testMySQL() = test(method, () => {
    for (db <- new MySQL("143.248.48.105/unit", "samples", "i int, name varchar(20), primary key(i)", "chrisjihee", "jiheeryu").manage()) {
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

  def testMongoDB() = test(method, () => {
    for (db <- new MongoDB("143.248.48.109/unit", "samples").manage()) {
      warn(s" + [DB] $db")
      db.delete(Filters.lte("_id", 1000))
      for (r <- Map("_id" -> 1, "name" -> "A") :: Map("_id" -> 2, "name" -> "B") :: Map("_id" -> 3, "name" -> "C") :: Nil)
        db.insert(r)
      db.update(Filters.eq("_id", 2), Updates.set("name", "BB"))
      db.replace(Filters.eq("_id", 3), Map("name" -> "CC"))

      val a0 = db.size
      warn(s"   - size : $a0")
      val a1 = db.one("name", Filters.eq("_id", 1))
      warn(s"   -  one : $a1")
      val a2 = db.ones("name", Filters.gte("_id", 2))
      warn(s"   - ones : ${a2.mkString(", ")}")
      val List(i, n) = db.row("_id, name", Filters.eq("_id", 1))
      warn(s"   -  row : $i->$n")
      val a4 = for (List(i, n) <- db.rows("_id, name", Filters.gte("_id", 2))) yield s"$i->$n"
      warn(s"   - rows : ${a4.mkString(", ")}")
      val a5 = db.map(Filters.eq("_id", 1))
      warn(s"   -  map : $a5")
      val a6 = for (r <- db.maps(Filters.gte("_id", 2))) yield r
      warn(s"   - maps : ${a6.mkString(", ")}")
    }
  })
}
