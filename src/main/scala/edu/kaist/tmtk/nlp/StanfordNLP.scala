package edu.kaist.tmtk.nlp

import java.util.Properties

import edu.kaist.tmtk.{log, quite2}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.time.SUTimeMain

import scala.collection.mutable.{LinkedHashMap => Map}

class StanfordNLP(components: String, lv: AnyRef = "W", q: Boolean = true, conf: Map[String, String] = Map(
  "tokenize.class" -> "PTBTokenizer",
  "pos.maxlen" -> "120",
  "parse.maxlen" -> "120"
)) {
  private val components2 = components.split(",").map(_.toLowerCase.trim).toList
  private val vannotators = "tokenize|cleanxml|ssplit|pos|lemma|ner|regexner|sentiment|truecase|parse|depparse|dcoref|relation|natlog|quote".split("\\|").toList
  val prop = new Properties
  prop.setProperty("annotators", components2.filter(vannotators.contains).mkString(", "))
  for ((k, v) <- conf)
    prop.setProperty(k, v)
  val analyzer = quite2(() => new StanfordCoreNLP(prop), q)
  val normalizer = if (components2.contains("normalize")) quite2(() => SUTimeMain.getPipeline(prop, true), q) else null
  override val toString = s"StanfordNLP($components)"
  log(s"[DONE] Load $this", lv)

  def analyze(text: String): Annotation = analyze(new Annotation(text))

  def analyze(annotation: Annotation): Annotation = {
    analyzer.annotate(annotation)
    annotation
  }

  def normalize(text: String, date: String) =
    SUTimeMain.textToAnnotatedXml(normalizer, text, date)
}

object StanfordNLP {
  def apply(components: String) =
    new StanfordNLP(components)

  def apply(components: String, q: Boolean) =
    new StanfordNLP(components, "W", q)

  def apply(components: String, lv: AnyRef) =
    new StanfordNLP(components, lv)

  def apply(components: String, lv: AnyRef, q: Boolean) =
    new StanfordNLP(components, lv, q)
}
