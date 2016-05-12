package edu.kaist.tmtk.nlp

import com.google.gson.Gson
import edu.kaist.tmtk._

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.mutable

class KoreanNLP(val addr: String, lv: AnyRef = "I") {
  val (host, port) = Option(addr.split(":")).map { case Array(a, b) => (a, b.asInt) }.get
  override val toString = s"KoreanNLP($addr)"
  log(s"[${if (usable1) "DONE" else "FAIL"}] Connect $this", lv)

  def usable1 = reconnectable(host, port, Stream(0))

  def usable2 = reconnectable(host, port, 0 #:: 1 #:: 2 #:: 3 #:: Stream(4))

  def parse(text: String, lv: AnyRef = "I", opt: String = ""): String = {
    var output: String = null
    log(s"    - ($addr) $opt / <text>${text.take(40).replaceAll("\r?\n", "\\\\n")}${if (text.length > 40) "..." else ""}</text>", lv)
    try {
      for (c <- reconnect(host, port)) {
        val writer = c.printWriter
        val reader = c.bufferedReader
        writer.write(text)
        writer.flush()
        output = Stream.iterate(reader.readLine)(x => reader.readLine).takeWhile(_ != null).mkString("\n").trim
      }
    } catch {
      case e: Exception => error(s"Error on parse: ${e.toString}\n($addr) $opt\n<text>$text</text>\n<output>$output</output>")
    }
    if (output == null)
      error(s"Empty output?: ($addr) $opt\n<text>$text</text>")
    output
  }

  def analyze(text: String): Packet =
    new Packet(parse(text))

  def analyzeOne(text: String) =
    Option(analyze(text)).map(_.sentences.head).get

  def tokenize(sent: ParsedSent) =
    sent.morp.map(_.lemma)

  def tokenize(text: String) =
    Option(analyze(text)).map(_.sentences.flatMap(_.morp.map(_.lemma))).get

  def detect(text: String) =
    Option(analyze(text)).map(_.sentences.map(_.text.trim)).get

  def detectAndTag(text: String) =
    Option(analyze(text)).map(_.sentences.map(x => x.text.trim + "\t" + x.morp.mkString(" "))).get

  def tag(text: String) =
    Option(analyze(text)).map(_.sentences.flatMap(_.morp.map(x => x.lemma + "/" + x.`type`))).get
}

object KoreanNLP {
  def apply(addr: String) =
    new KoreanNLP(addr)
}

class Packet(val output: String, input: String = "", opt: String = "") {
  val paragraphs = try {
    if (output != null && output.startsWith("<parsed>") && output.endsWith("</parsed>"))
      output.replaceAll("^<parsed>|</parsed>$", "").split("</parsed><parsed>").toSeq.map(new Gson().fromJson(_, classOf[ParsedText]))
    else null
  } catch {
    case e: Exception => error(s"Error on Packet: ${e.toString}\n<raw>$output</raw>"); null
  }
  val sentences = if (paragraphs != null) paragraphs.flatMap(_.sentence) else null
  if (paragraphs == null)
    error(s"Empty parses?: <output>$output</output><input>$input</input><opt>$opt</opt>")
  else if (sentences == null)
    error(s"Empty sentences?: <output>$output</output><input>$input</input><opt>$opt</opt>")
}

case class ParsedText(sentence: JList[ParsedSent]) {
  override lazy val toString = Seq("sentence" -> sentence).map { case (k, v) => s"$k: $v" }.mkString(", ")
}

case class ParsedSent(text: String, morp: JList[ParsedMorp], morp_eval: JList[ParsedMorpEval], word: JList[ParsedWord],
                      WSD: JList[ParsedWSD], NE: JList[ParsedNE], dependency: JList[ParsedDep], SRL: JList[ParsedSRL], ZA: JList[ParsedZA]) {
  override lazy val toString = Seq("text" -> text, "morp" -> morp, "morp_eval" -> morp_eval, "word" -> word, "WSD" -> WSD, "NE" -> NE, "dependency" -> dependency, "SRL" -> SRL, "ZA" -> ZA)
    .map { case (k, v) => s"$k: $v" }.mkString(", ")

  def dependents(gov: ParsedDep): mutable.Buffer[ParsedDep] = {
    val sub = for (dep <- gov.mod.map(_.asInt).toBuffer.map(dep).filter(_ != null))
      yield dependents(dep) += dep
    sub.flatten
  }

  def dep(id: Int) = dependency.find(_.id == id).orNull
}

case class ParsedMorp(id: Int, lemma: String, `type`: String, position: Int, weight: Double) {
  override lazy val toString = Seq(lemma, `type`, position).mkString("/")
}

case class ParsedMorpEval(id: Int, result: String, target: String, word_id: Int, m_begin: Int, m_end: Int) {
  override lazy val toString = Seq(result, target, word_id, m_begin, m_end).mkString("/")
}

case class ParsedWord(id: Int, var text: String, `type`: String, begin: Int, end: Int) {
  override lazy val toString = Seq(text, begin, end).mkString("/")
}

case class ParsedWSD(id: Int, var text: String, `type`: String, scode: String, weight: Double, position: Int, begin: Int, end: Int) {
  override lazy val toString = Seq(text, `type`, scode, position, begin, end).mkString("/")
}

case class ParsedNE(id: Int, var text: String, `type`: String, begin: Int, end: Int, weight: Double, common_noun: Int) {
  override lazy val toString = Seq(text, `type`, begin, end, common_noun).mkString("/")
}

case class ParsedDep(id: Int, var text: String, head: Int, label: String, mod: JList[Integer], weight: Double) {
  override lazy val toString = Seq(text, label, head, mod.mkString(",")).mkString("/")
}

case class ParsedSRL(verb: String, sense: Int, word_id: Int, weight: Double, argument: JList[ParsedArg]) {
  override lazy val toString = Seq(verb, sense, argument.mkString(","), word_id).mkString("/")
}

case class ParsedZA(id: Int, verb_wid: Int, ant_sid: Int, ant_wid: Int, `type`: String, istitle: Int, weight: Double) {
  override lazy val toString = Seq(`type`, ant_wid, ant_sid, verb_wid).mkString("/")
}

case class ParsedArg(`type`: String, word_id: Int, text: String, weight: Double) {
  override val toString = Seq(`type`, text, word_id).mkString(":")
}
