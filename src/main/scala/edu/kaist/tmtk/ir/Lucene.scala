package edu.kaist.tmtk.ir

import java.io.{Closeable, File}

import edu.kaist.tmtk._
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.Field.Store.YES
import org.apache.lucene.document._
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import resource.managed

import scala.collection.Map

class Lucene(path: Any, analyzer: Analyzer) extends Closeable {
  val path2 = path match {
    case x: String => new File(x)
    case x: File => x
  }
  lazy val writer = new IndexWriter(FSDirectory.open(path2.toPath), new IndexWriterConfig(analyzer))
  lazy val searcher = new IndexSearcher(DirectoryReader.open(writer))
  override val toString = s"Lucene($path with ${analyzer.getClass.getSimpleName})"

  override def close() = {
    writer.commit()
    writer.close()
  }

  def manage() = managed(this)

  def clear() = {
    writer.deleteAll()
    this
  }

  def insert(row: Map[String, Any]) = {
    val d = new Document
    for ((k, v) <- row)
      d.add(v match {
        case x: Int => new IntField(k, x, YES)
        case x: Long => new LongField(k, x, YES)
        case x: Float => new FloatField(k, x, YES)
        case x: Double => new DoubleField(k, x, YES)
        case x: String if k.startsWith("t_") => new TextField(k, x, YES)
        case x: String if !k.startsWith("t_") => new StringField(k, x, YES)
      })
    writer.addDocument(d)
  }

  def commit() = {
    writer.commit()
    this
  }

  private def format(query: String, args: Seq[Any]) =
    new QueryParser("X", analyzer).parse(query.replace("%", "%%").replace("<?>", "%s").trim.format(args: _*))

  def size = count("*:*")

  def count(query: String, args: Any*) =
    searcher.count(format(query, args))

  def one(field: String, query: String, args: Any*) =
    (for (d <- searcher.search(format(query, args), 1).scoreDocs.headOption)
      yield searcher.doc(d.doc)(field)).orNull

  def ones(k: Int, field: String, query: String, args: Any*) =
    for (d <- searcher.search(format(query, args), k).scoreDocs.toStream)
      yield searcher.doc(d.doc)(field)

  def row(fields: String, query: String, args: Any*) =
    (for (d <- searcher.search(format(query, args), 1).scoreDocs.headOption)
      yield
        if (fields == "*") searcher.doc(d.doc).values ++ Seq(d.score)
        else searcher.doc(d.doc).values(fields.split(",").map(_.trim)) ++ Seq(d.score)
      ).orNull

  def rows(k: Int, fields: String, query: String, args: Any*) =
    for (d <- searcher.search(format(query, args), k).scoreDocs.toStream)
      yield
        if (fields == "*") searcher.doc(d.doc).values ++ Seq(d.score)
        else searcher.doc(d.doc).values(fields.split(",").map(_.trim)) ++ Seq(d.score)

  def map(query: String, args: Any*) =
    (for (d <- searcher.search(format(query, args), 1).scoreDocs.headOption)
      yield XMap(searcher.doc(d.doc).items ++ Seq("score" -> d.score): _*)).orNull

  def maps(k: Int, query: String, args: Any*) =
    for (d <- searcher.search(format(query, args), k).scoreDocs.toStream)
      yield XMap(searcher.doc(d.doc).items ++ Seq("score" -> d.score): _*)
}
