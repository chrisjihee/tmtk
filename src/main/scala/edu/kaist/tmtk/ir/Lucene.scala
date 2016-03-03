package edu.kaist.tmtk.ir

import java.io.File

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store.YES
import org.apache.lucene.document.{Document, DoubleField, FloatField, IntField, LongField, StringField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory

import scala.collection.Map
import scala.collection.immutable.ListMap

class Lucene(path: Any, analyzer: Analyzer = new StandardAnalyzer) {
  val path2 = path match {
    case x: String => new File(x)
    case x: File => x
  }
  lazy val writer = new IndexWriter(FSDirectory.open(path2.toPath), new IndexWriterConfig(analyzer))
  lazy val searcher = new IndexSearcher(DirectoryReader.open(writer))
  lazy val parser = new QueryParser("X", analyzer)
  override val toString = s"Lucene($path with ${analyzer.getClass.getSimpleName})"

  def clear() = writer.deleteAll()

  def insert(row: Map[String, Any]) = {
    val d = new Document
    for ((k, v) <- row) d.add(v match {
      case x: Int => new IntField(k, x, YES)
      case x: Long => new LongField(k, x, YES)
      case x: Float => new FloatField(k, x, YES)
      case x: Double => new DoubleField(k, x, YES)
      case x: String if k.startsWith("t_") => new TextField(k, x, YES)
      case x: String if !k.startsWith("t_") => new StringField(k, x, YES)
    })
    writer.addDocument(d)
  }

  def commit() = writer.commit()

  private def format(query: String, args: Seq[Any]) =
    parser.parse(query.replace("?", "%s").trim.format(args: _*))

  def count(query: String, args: Any*) =
    searcher.count(format(query, args))

  def one(name: String, query: String, args: Any*) =
    (for (d <- searcher.search(format(query, args), 1).scoreDocs.headOption)
      yield searcher.doc(d.doc)(name)).orNull

  def ones(k: Int, name: String, query: String, args: Any*) =
    for (d <- searcher.search(format(query, args), k).scoreDocs.toStream)
      yield searcher.doc(d.doc)(name)

  def row(query: String, args: Any*) =
    (for (d <- searcher.search(format(query, args), 1).scoreDocs.headOption)
      yield searcher.doc(d.doc).values ++ Seq(d.score)).orNull

  def rows(k: Int, query: String, args: Any*) =
    for (d <- searcher.search(format(query, args), k).scoreDocs.toStream)
      yield searcher.doc(d.doc).values ++ Seq(d.score)

  def map(query: String, args: Any*) =
    (for (d <- searcher.search(format(query, args), 1).scoreDocs.headOption)
      yield ListMap(searcher.doc(d.doc).items.toSeq ++ Seq("score" -> d.score): _*)).orNull

  def maps(k: Int, query: String, args: Any*) =
    for (d <- searcher.search(format(query, args), k).scoreDocs.toStream)
      yield ListMap(searcher.doc(d.doc).items.toSeq ++ Seq("score" -> d.score): _*)
}
