package edu.kaist.tmtk

import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexableField

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.Map

package object ir {

  implicit class FieldOps(x: IndexableField) {
    def value =
      Option(x).map(x => (x.numericValue, x.stringValue)).map { case (n, s) => if (n != null) n else s }.orNull

    def item =
      Option(x).map(x => (x.name, x.numericValue, x.stringValue)).map { case (k, n, s) => (k, if (n != null) n else s) }.orNull
  }

  implicit class DocumentOps(x: Document) {
    def apply(name: String) =
      x.getField(name).value

    def values =
      x.getFields.map(_.value)

    def items =
      x.getFields.map(_.item)
  }

  def main(args: Array[String]) {
    args.at(0, null) match {
      case "Lucene" => testLucene()
      case _ =>
    }
  }

  def testLucene() = test(method, () => {
    val ir = new Lucene(inTemp("test-index"))
    warn(s" + [IR] $ir")
    ir.clear()
    for (r <- Map("i" -> 1, "name" -> "A", "t_text" -> "Apple is good for health") :: Map("i" -> 2, "name" -> "B", "t_text" -> "Banana is good for heart") :: Map("i" -> 3, "name" -> "C", "t_text" -> "Carrot is good for eyesight") :: Nil)
      ir.insert(r)
    ir.commit()

    val a0 = ir.count("*:*")
    warn(s"   - count : $a0")
    val a1 = ir.one("name", "t_text:?", "eyesight")
    warn(s"   -   one : $a1")
    val a2 = ir.ones(10, "name", "t_text:?", "good")
    warn(s"   -  ones : ${a2.mkString(", ")}")
    val List(i, n, t, s) = ir.row("t_text:?", "eyesight")
    warn(s"   -   row : $i->($n, $t) ($s)")
    val a4 = for (List(i, n, t, s) <- ir.rows(10, "t_text:?", "good")) yield s"$i->($n, $t) ($s)"
    warn(s"   -  rows : ${a4.mkString(", ")}")
    val a5 = ir.map("t_text:?", "eyesight")
    warn(s"   -   map : $a5")
    val a6 = for (r <- ir.maps(10, "t_text:?", "good")) yield r
    warn(s"   -  maps : ${a6.mkString(", ")}")
  })
}
