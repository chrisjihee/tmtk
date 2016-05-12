package edu.kaist.tmtk.ie

import edu.kaist.tmtk._
import edu.knowitall.ollie.Ollie
import edu.knowitall.ollie.confidence.OllieConfidenceFunction
import edu.knowitall.tool.parse.MaltParser

class OpenIE(lv: AnyRef = "W", q: Boolean = true, conf: AMap[String, String] = XMap("dep.model" -> "maltparser/engmalt.linear-1.7.mco")) {
  System.setProperty("Malt.verbosity", "WARN")
  val parser = quite2(() => new MaltParser(findFile(conf("dep.model")).toURL), q)
  val extractor = new Ollie
  val confidence = OllieConfidenceFunction.loadDefaultClassifier()
  override val toString = s"OpenIE(${extractor.getClass.getSimpleName})"
  log(s"[DONE] Load $this", lv)

  class Extraction(val arg1: String, val rel: String, val arg2: String, val attribution: String, val enabler: String, val score: Double) {
    override def toString = Seq(arg1, rel, arg2, attribution, enabler, "%.4f" format score).mkString("\t")
  }

  def extract(sentence: String) = {
    val tagged = parser.postagger.postag(sentence)
    val parsed = parser.dependencyGraphPostagged(tagged)
    val extracted = for (e <- extractor.extract(parsed)) yield
      new Extraction(e.extraction.arg1.text, e.extraction.rel.text, e.extraction.arg2.text, e.extraction.attribution.map(_.text).orNull, e.extraction.enabler.map(_.text).orNull, confidence(e))
    extracted.toSeq
  }
}

object OpenIE {
  def apply() =
    new OpenIE

  def apply(q: Boolean) =
    new OpenIE("W", q)

  def apply(lv: AnyRef) =
    new OpenIE(lv)

  def apply(lv: AnyRef, q: Boolean) =
    new OpenIE(lv, q)
}
