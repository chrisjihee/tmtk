package edu.kaist.tmtk.nlp

import com.google.gson.Gson
import edu.kaist.tmtk.{AsValue, log, reconnect, toJavaList => list}

import scala.collection.JavaConversions.collectionAsScalaIterable

class KoreanNLP(addr: String, lv: AnyRef = "I") {
  val Array(host, port) = addr.split(":")
  override val toString = s"KoreanNLP($addr)"
  log(s"[DONE] Connect $this", lv)

  def analyze(text: String) = {
    val p = for {
      connection <- Option(reconnect(host, port.asInt))
      writer = connection.printWriter
      reader = connection.bufferedReader
    } yield {
      writer.write(text)
      writer.flush()
      val r = Stream.iterate(reader.readLine)(x => reader.readLine).takeWhile(_ != null).mkString("\n")
      connection.close()
      new Packet(r)
    }
    p.orNull
  }

  def analyzeOne(text: String) =
    analyze(text).sentences.head

  def tokenize(sent: ParsedSent) =
    sent.morp.map(_.lemma)

  def tokenize(text: String) =
    analyze(text).sentences.flatMap(_.morp.map(_.lemma))

  def detect(text: String) =
    analyze(text).sentences.map(_.text)

  def tag(text: String) =
    analyze(text).sentences.flatMap(_.morp.map(x => x.lemma + "/" + x.`type`))
}

object KoreanNLP {
  def apply(addr: String) =
    new KoreanNLP(addr)
}

class Packet(raw: String) {
  private val rxResult = """(?s)<result level="[0-9]+">(.+)</result>""".r
  val rxResult(json) = raw
  private val cont = new Gson().fromJson(json.trim, classOf[Content])
  val sentences = cont.sentence
}

class Content {
  val sentence = list(new ParsedSent)
}

class ParsedSent {
  val text = ""
  val morp = list(new ParsedMorp)
  val morp_eval = list(new ParsedMorpEval)
  val word = list(new ParsedWord)
  val WSD = list(new ParsedWSD)
  val NE = list(new ParsedNE)
  val dependency = list(new ParsedDep)
}

class ParsedMorp {
  val id = 0
  val lemma = ""
  val `type` = ""
  val position = 0
  val weight = 0.0

  override def toString = Seq(lemma, `type`, position).mkString("/")
}

class ParsedMorpEval {
  val id = 0
  val result = ""
  val target = ""
  val word_id = 0
  val m_begin = 0
  val m_end = 0

  override def toString = Seq(result, target, word_id, m_begin, m_end).mkString("/")
}

class ParsedWord {
  val id = 0
  val text = ""
  val `type` = ""
  val begin = 0
  val end = 0

  override def toString = Seq(text, begin, end).mkString("/")
}

class ParsedWSD {
  val id = 0
  val text = ""
  val `type` = ""
  val scode = ""
  val weight = 0.0
  val position = 0
  val begin = 0
  val end = 0

  override def toString = Seq(text, `type`, scode, position, begin, end).mkString("/")
}

class ParsedNE {
  val id = 0
  val text = ""
  val `type` = ""
  val begin = 0
  val end = 0
  val weight = 0.0
  val common_noun = 0

  override def toString = Seq(text, `type`, begin, end, common_noun).mkString("/")
}

class ParsedDep {
  val id = 0
  val text = ""
  val head = 0
  val label = ""
  val mod = list(0)
  val weight = 0.0

  override def toString = Seq(text, label, head, mod.mkString(",")).mkString("/")
}
