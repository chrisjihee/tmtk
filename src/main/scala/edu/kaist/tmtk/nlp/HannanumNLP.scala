package edu.kaist.tmtk.nlp

import java.io.Closeable

import edu.kaist.tmtk.log
import kr.ac.kaist.swrc.jhannanum.comm.{PlainSentence, Sentence}
import kr.ac.kaist.swrc.jhannanum.hannanum.Workflow
import kr.ac.kaist.swrc.jhannanum.plugin.MajorPlugin.MorphAnalyzer.ChartMorphAnalyzer.ChartMorphAnalyzer
import kr.ac.kaist.swrc.jhannanum.plugin.MajorPlugin.PosTagger.HmmPosTagger.HMMTagger
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.MorphemeProcessor.UnknownMorphProcessor.UnknownProcessor
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.PlainTextProcessor.SentenceSegmentor.SentenceSegmentor
import kr.ac.kaist.swrc.jhannanum.plugin.SupplementPlugin.PosProcessor.SimplePOSResult22.SimplePOSResult22

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.immutable.ListMap

class HannanumNLP(components: String, lv: AnyRef = "W", conf: Map[String, String] = ListMap(
  "morp.json" -> "rsc/hannanum/ChartMorphAnalyzer.json",
  "pos.json" -> "rsc/hannanum/HmmPosTagger.json"
)) extends Closeable {
  private val components2 = components.split(",").map(_.toLowerCase.trim).toList
  val detector = {
    if (components2.contains("ssplit")) {
      val processor = new Workflow
      processor.appendPlainTextProcessor(new SentenceSegmentor, null)
      processor.activateWorkflow(true)
      processor
    } else null
  }
  val tagger = {
    if (components2.contains("pos")) {
      val processor = new Workflow
      processor.setMorphAnalyzer(new ChartMorphAnalyzer, conf("morp.json"))
      processor.appendMorphemeProcessor(new UnknownProcessor, null)
      processor.setPosTagger(new HMMTagger, conf("pos.json"))
      processor.appendPosProcessor(new SimplePOSResult22, null)
      processor.activateWorkflow(true)
      processor
    } else null
  }
  override val toString = s"HannanumNLP($components)"
  log(s"[DONE] Load $this", lv)

  override def close() = {
    if (detector != null) detector.close()
    if (tagger != null) tagger.close()
  }

  def detect(text: String) = {
    detector.analyze(text)
    detector.getResultOfDocument(new PlainSentence(0, 0, true)).map(_.getSentence)
  }

  def tag(sent: String) = {
    tagger.analyze(sent)
    val words = tagger.getResultOfDocument(new Sentence(0, 0, true)).head.getEojeols
    words.map(x => x.getMorphemes.zip(x.getTags).map(x => x._1 + "/" + x._2)).map(_.mkString("+"))
  }
}

object HannanumNLP {
  def apply(components: String) =
    new HannanumNLP(components)

  def apply(components: String, lv: AnyRef) =
    new HannanumNLP(components, lv)
}
