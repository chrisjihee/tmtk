package edu.kaist.tmtk.nlp

import java.util.{List => JList}

import edu.emory.clir.clearnlp.component.mode.dep.DEPConfiguration
import edu.emory.clir.clearnlp.component.mode.srl.SRLConfiguration
import edu.emory.clir.clearnlp.component.utils.{GlobalLexica, NLPUtils}
import edu.emory.clir.clearnlp.dependency.{DEPNode, DEPTree}
import edu.emory.clir.clearnlp.util.lang.TLanguage.ENGLISH
import edu.kaist.tmtk.{log, toInputStream}

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

  def detect(text: String) =
    tokenizer.segmentize(toInputStream(text))

  def tagWord(toks: Iterable[String]) =
    process(toks).map(x => x.getWordForm + "/" + x.getPOSTag)

  def tagWord(toks: Array[String]) =
    process(toks).map(x => x.getWordForm + "/" + x.getPOSTag)

  def tagWord(toks: JList[String]) =
    process(toks).map(x => x.getWordForm + "/" + x.getPOSTag)

  def tagLemma(toks: Iterable[String]) =
    process(toks).map(x => x.getLemma + "/" + x.getPOSTag)

  def tagLemma(toks: Array[String]) =
    process(toks).map(x => x.getLemma + "/" + x.getPOSTag)

  def tagLemma(toks: JList[String]) =
    process(toks).map(x => x.getLemma + "/" + x.getPOSTag)

  def process(toks: Iterable[String]): DEPTree = process(asTree(toks))

  def process(toks: Array[String]): DEPTree = process(asTree(toks))

  def process(toks: JList[String]): DEPTree = process(asTree(toks))

  def process(tree: DEPTree) = {
    for (annotator <- annotators)
      annotator.process(tree)
    tree
  }

  private def asTree(toks: Iterable[String]) = {
    val tree = new DEPTree(toks.size)
    for ((tok, i) <- toks.zipWithIndex)
      tree.add(new DEPNode(i + 1, tok))
    tree
  }

  def analyze(text: String) = detect(text).map(process)
}

object ClearNLP {
  def apply(components: String) =
    new ClearNLP(components)

  def apply(components: String, lv: AnyRef) =
    new ClearNLP(components, lv)
}
