package edu.kaist.tmtk.nlp

import edu.emory.clir.clearnlp.component.mode.dep.DEPConfiguration
import edu.emory.clir.clearnlp.component.mode.srl.SRLConfiguration
import edu.emory.clir.clearnlp.component.utils.{GlobalLexica, NLPUtils}
import edu.emory.clir.clearnlp.dependency.{DEPNode, DEPTree}
import edu.emory.clir.clearnlp.util.lang.TLanguage.ENGLISH
import edu.kaist.tmtk.{error, log, toInputStream}

import scala.annotation.tailrec
import scala.collection.JavaConversions.{iterableAsScalaIterable, seqAsJavaList}
import scala.collection.mutable.{LinkedHashMap => Map}

class ClearNLP(components: String, lv: AnyRef = "W", conf: Map[String, String] = Map(
  "lexica" -> "brown-rcv1.clean.tokenized-CoNLL03.txt-c1000-freq1.txt.xz",
  "gazetteer" -> "general-en-ner-gazetteer.xz",
  "pos.model" -> "general-en-pos.xz",
  "dep.model" -> "general-en-dep.xz",
  "srl.model" -> "general-en-srl.xz",
  "ner.model" -> "general-en-ner.xz"
)) {
  private val components2 = components.split(",").map(_.toLowerCase.trim).toList
  val tokenizer = {
    (for (lang <- Some(ENGLISH) if components2.contains("tokenize") || components2.contains("ssplit"))
      yield NLPUtils.getTokenizer(lang)).orNull
  }
  val tagger = {
    if (components2.contains("pos") || components2.contains("dep") || components2.contains("ner"))
      GlobalLexica.initDistributionalSemanticsWords(List(conf("lexica")))
    (for (lang <- Some(ENGLISH) if components2.contains("pos"))
      yield NLPUtils.getPOSTagger(lang, conf("pos.model"))).orNull
  }
  val lemmatizer = {
    (for (lang <- Some(ENGLISH) if components2.contains("lemma"))
      yield NLPUtils.getMPAnalyzer(lang)).orNull
  }
  val parser = {
    (for (lang <- Some(ENGLISH) if components2.contains("dep"))
      yield NLPUtils.getDEPParser(lang, conf("dep.model"), new DEPConfiguration("root"))).orNull
  }
  val labeler = {
    (for (lang <- Some(ENGLISH) if components2.contains("srl"))
      yield NLPUtils.getSRLabeler(lang, conf("srl.model"), new SRLConfiguration(4, 3))).orNull
  }
  val recognizer = {
    (for (lang <- Some(ENGLISH) if components2.contains("ner"))
      yield {
        GlobalLexica.initNamedEntityDictionary(conf("gazetteer"))
        NLPUtils.getNERecognizer(lang, conf("ner.model"))
      }).orNull
  }
  val annotators = List(tagger, lemmatizer, parser, labeler, recognizer).filter(_ != null)
  override val toString = s"ClearNLP($components)"
  log(s"[DONE] Load $this", lv)

  def tokenize(text: String): Iterable[String] =
    tokenizer.tokenize(text).toIterable

  def detect(text: String): Iterable[Iterable[String]] =
    tokenizer.segmentize(toInputStream(text)).map(_.toIterable)

  def tagWord(toks: Iterable[String]) =
    analyze(toks).map(x => x.getWordForm + "/" + x.getPOSTag)

  def tagLemma(toks: Iterable[String]) =
    analyze(toks).map(x => x.getLemma + "/" + x.getPOSTag)

  def analyze(text: String): Iterable[DEPTree] = detect(text).map(analyze)

  def analyze(toks: Iterable[String]): DEPTree = analyze(toTree(toks))

  @tailrec
  final def analyze(tree: DEPTree, limit: Int = 3, times: Int = 1): DEPTree = try {
    for (annotator <- annotators)
      annotator.process(tree)
    tree
  }
  catch {
    case e: Throwable => if (times > limit) tree
    else {
      error(s"Thrown exception: ${e.getClass.getSimpleName}: ${e.getMessage}")
      Thread.sleep(1000)
      analyze(tree, times = times + 1)
    }
  }

  private def toTree(toks: Iterable[String]) = {
    val tree = new DEPTree(toks.size)
    for ((tok, i) <- toks.zipWithIndex)
      tree.add(new DEPNode(i + 1, tok))
    tree
  }
}
