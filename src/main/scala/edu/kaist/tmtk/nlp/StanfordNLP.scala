package edu.kaist.tmtk.nlp

import java.util.Properties

import edu.kaist.tmtk.{log, quite2}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

import scala.collection.mutable.{LinkedHashMap => Map}

class StanfordNLP(components: String, lv: AnyRef = "W", conf: Map[String, String] = Map(
  "tokenize.class" -> "PTBTokenizer",
  "pos.maxlen" -> "120",
  "parse.maxlen" -> "120"
)) {
  val analyzer = quite2(() => new StanfordCoreNLP({
    val p = new Properties
    p.setProperty("annotators", components)
    for ((k, v) <- conf)
      p.setProperty(k, v)
    p
  }))
  override val toString = s"StanfordNLP($components)"
  log(s"[DONE] Load $this", lv)

  def analyze(text: String): Annotation = analyze(new Annotation(text))

  def analyze(annotation: Annotation): Annotation = {
    analyzer.annotate(annotation)
    annotation
  }
}
