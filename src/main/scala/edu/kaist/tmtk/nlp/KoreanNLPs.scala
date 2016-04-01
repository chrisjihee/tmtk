package edu.kaist.tmtk.nlp

import edu.kaist.tmtk.log

class KoreanNLPs(addrs: Seq[String], lv: AnyRef = "I") {
  val nlps = for (addr <- addrs) yield new KoreanNLP(addr, lv)
  val free = nlps.toBuffer.filter(_ != null)
  override val toString = s"KoreanNLPs(${addrs.mkString(", ")})"
  log(s"[DONE] Ready $this", lv)

  def use[R](f: KoreanNLP => R) = {
    while (free.isEmpty)
      Thread.sleep(500)
    val nlp = free.synchronized(free.remove(0))
    val r = f(nlp)
    free.synchronized(free.append(nlp))
    r
  }

  def size = nlps.size
}
