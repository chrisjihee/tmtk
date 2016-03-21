package edu.kaist.tmtk.nlp

import edu.kaist.tmtk.{findFile, log}
import edu.stanford.nlp.util.CoreMap
import opennlp.tools.chunker.{ChunkerME, ChunkerModel}
import opennlp.tools.cmdline.parser.ParserTool
import opennlp.tools.namefind.{NameFinderME, NameSample, TokenNameFinderModel}
import opennlp.tools.parser.{Parse, ParserFactory, ParserModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

import scala.collection.immutable.ListMap

class OpenNLP(components: String, lv: AnyRef = "W", conf: Map[String, String] = ListMap(
  "sent.model" -> "opennlp/en-sent.bin",
  "tok.model" -> "opennlp/en-token.bin",
  "pos.model" -> "opennlp/en-pos-maxent.bin",
  "chunk.model" -> "opennlp/en-chunker.bin",
  "parse.model" -> "opennlp/en-parser-chunking.bin",
  "ner.person.model" -> "opennlp/en-ner-person.bin",
  "ner.organization.model" -> "opennlp/en-ner-organization.bin",
  "ner.location.model" -> "opennlp/en-ner-location.bin",
  "ner.date.model" -> "opennlp/en-ner-date.bin",
  "ner.time.model" -> "opennlp/en-ner-time.bin",
  "ner.money.model" -> "opennlp/en-ner-money.bin",
  "ner.percentage.model" -> "opennlp/en-ner-percentage.bin"
)) {
  private val components2 = components.split(",").map(_.toLowerCase.trim).toList
  val tokenizer = {
    (for (file <- Option(findFile(conf("tok.model"))) if components2.contains("tokenize"))
      yield new TokenizerME(new TokenizerModel(file.toURL))).orNull
  }
  val detector = {
    (for (file <- Option(findFile(conf("sent.model"))) if components2.contains("ssplit"))
      yield new SentenceDetectorME(new SentenceModel(file.toURL))).orNull
  }
  val tagger = {
    (for (file <- Option(findFile(conf("pos.model"))) if components2.contains("pos"))
      yield new POSTaggerME(new POSModel(file.toURL))).orNull
  }
  val chunker = {
    (for (file <- Option(findFile(conf("chunk.model"))) if components2.contains("chunk"))
      yield new ChunkerME(new ChunkerModel(file.toURL))).orNull
  }
  val parser = {
    (for (file <- Option(findFile(conf("parse.model"))) if components2.contains("parse"))
      yield ParserFactory.create(new ParserModel(file.toURL))).orNull
  }
  val recognizers = {
    for ((k, v) <- conf if components2.contains("ner") && k.startsWith("ner.") && k.endsWith(".model"); file = findFile(v))
      yield new NameFinderME(new TokenNameFinderModel(file.toURL))
  }
  override val toString = s"OpenNLP($components)"
  log(s"[DONE] Load $this", lv)

  def tokenize(text: String) =
    tokenizer.tokenize(text)

  def detect(text: String) =
    detector.sentDetect(text)

  def tag(toks: Array[String]): Array[String] =
    tagger.tag(toks)

  def tag(toks: Iterable[String]): Array[String] =
    tag(toks.toArray)

  def tag(sent: CoreMap): Array[String] =
    tag(sent.words)

  def chunk(toks: Array[String], tags: Array[String]): Array[String] =
    chunker.chunk(toks, tags)

  def chunk(toks: Iterable[String], tags: Iterable[String]): Array[String] =
    chunk(toks.toArray, tags.toArray)

  def chunk(sent: CoreMap): Array[String] =
    chunk(sent.words, sent.tags)

  def chunkToString(toks: Array[String], tags: Array[String]): String =
    toChunkString(toks, tags, chunk(toks, tags))

  def chunkToString(toks: Iterable[String], tags: Iterable[String]): String =
    chunkToString(toks.toArray, tags.toArray)

  def chunkToString(sent: CoreMap): String =
    chunkToString(sent.words, sent.tags)

  def parse(sent: String, k: Int): Array[Parse] =
    ParserTool.parseLine(sent, parser, k)

  def parse(sent: String): Parse =
    parse(sent, 1)(0)

  def parseToString(sent: String, k: Int): Array[String] =
    parse(sent, k).map(toTreeString)

  def parseToString(sent: String): String =
    toTreeString(parse(sent))

  def recognize(toks: Array[String]): String = {
    val spans = recognizers.flatMap(_.find(toks))
    new NameSample(toks, NameFinderME.dropOverlappingSpans(spans.toArray), false).toString
  }

  def recognize(toks: Iterable[String]): String =
    recognize(toks.toArray)

  def recognize(sent: CoreMap): String =
    recognize(sent.words)

  def clearRecognizer() =
    recognizers.foreach(_.clearAdaptiveData)
}

object OpenNLP {
  def apply(components: String) =
    new OpenNLP(components)

  def apply(components: String, lv: AnyRef) =
    new OpenNLP(components, lv)
}
