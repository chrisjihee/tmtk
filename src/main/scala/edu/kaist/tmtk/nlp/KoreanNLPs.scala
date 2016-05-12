package edu.kaist.tmtk.nlp

import java.util.concurrent.ArrayBlockingQueue

import edu.kaist.tmtk._

import scala.collection.JavaConversions._

class KoreanNLPs(addrs: Seq[String], lv: AnyRef = "I", lv2: AnyRef = "I", factor: Float = 1.0f) {
  val nlps = for (addr <- addrs) yield new KoreanNLP(addr, lv2)
  val free = new ArrayBlockingQueue[KoreanNLP]((nlps.size * factor).asInt)
  for (x <- nlps if x.usable1) free.add(x)
  override val toString = s"KoreanNLPs(${free.iterator.map(_.addr).mkString(", ")})"
  if (free.size > 0)
    log(s"[DONE] Ready ${free.size} $this", lv)
  else
    fatal(s"[FAIL] No available server")

  def size = nlps.size

  def get = free.take

  def put(nlp: KoreanNLP) = free.add(nlp)

  def packet(text: String, lv: AnyRef = "I", opt: String = "") =
    new Packet(use(_.parse(text, lv, opt = opt)), text, opt)

  def use[R](f: KoreanNLP => R) = {
    var nlp = get
    while (!nlp.usable2)
      nlp = get
    val r = f(nlp)
    put(nlp)
    r
  }
}
