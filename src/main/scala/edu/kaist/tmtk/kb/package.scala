package edu.kaist.tmtk

import java.util.concurrent.atomic.AtomicInteger

import de.tudarmstadt.ukp.wikipedia.api.Page
import de.tudarmstadt.ukp.wikipedia.api.exception.{WikiPageNotFoundException, WikiTitleParsingException}
import edu.kaist.tmtk.db.Cassandra

import scala.reflect.io.File
import scala.util.Properties

package object kb {

  implicit class PageOps(page: Page) {
    private val removable_sections = ("See also;Notes and references;Notes;Footnotes;Citations;References;Bibliography;" +
      "Further reading;External links;Sources;Sources/external links;" +
      "참고문헌;참고 문헌;참고 자료;참조;주석과 참고 자료;관련 항목;관련 분야;바깥 고리;바깥고리;외부 고리;바깥링크;외부 연결;" +
      "같이 보기;같이보기;같이 읽기;함께 보기;각주;주해;주석;읽어보기;읽을거리;출처").split(";")
    private val removable_sections_regex = s"""(?is)==[= 	]*(${removable_sections.mkString("|")})[= 	]*==.*$$""".r

    def isRegular = try {
      !page.isDisambiguation && !page.isDiscussion && !page.isRedirect
    } catch {
      case e: WikiTitleParsingException => false
    }

    def getLinkedText = try {
      var text = page.getText

      text = "^:.+".r.replaceAllIn(text, "") // 특별 시작문구 제거

      text = """\{\{(nihongo)\|([^|\}]+)\|([^|\}]+)[^\}]*\}\}""".r.replaceAllIn(text, "$2")
      text = """\{\{(lang)\|([^|\}]+)\|([^|\}]+)[^\}]*\}\}""".r.replaceAllIn(text, "$3")
      text = """\{\{(convert)\|([^|\}]+)\|([^|\}]+)[^\}]*\}\}""".r.replaceAllIn(text, "$2$3")

      text = text.replace("&nbsp;", " ")
      text = text.replace("&ndash;", "–")
      text = text.replace("C-change", "°C")
      text = text.replace("F-change", "°F")
      text = text.replace("'''", "")
      text = text.replace("''", "")

      text = text.replace("<!--", "〔")
      text = text.replace("-->", "〕")
      for (i <- 1 to 10)
        text = "〔[^〔〕]*〕".r.replaceAllIn(text, "")

      text = text.replace("{{", "《")
      text = text.replace("}}", "》")
      for (i <- 1 to 10)
        text = "《[^《》]*》".r.replaceAllIn(text, "")

      text = text.replace("{", "〈")
      text = text.replace("}", "〉")
      for (i <- 1 to 10)
        text = "〈[^〈〉]*〉".r.replaceAllIn(text, "")

      text = text.replaceAll("<ref>|<ref[^>/]*>", "『")
      text = text.replaceAll("</ref>", "』")
      text = text.replaceAll("<gallery>|<gallery ", "『")
      text = text.replaceAll("</gallery>", "』")
      for (i <- 1 to 10)
        text = "『[^『』]*』".r.replaceAllIn(text, "")

      text = text.replaceAll("</?[^/<>]*?/?>", "") // 모든 태그 제거

      text = """(?i)\[\[(File|Image|파일|이미지):[^\[\]]*(\[\[[^\[\]]*\]\][^\[\]]*)*\]\]""".r.replaceAllIn(text, "") // 파일, 이미지 제거
      text = """(?i)\[\[(Category|분류):.*\]\]""".r.replaceAllIn(text, "") // 카테고리 제거
      text = """(?i)\[[a-z]+://[^ \]]+\]\.?""".r.replaceAllIn(text, "") // URI 제거
      text = """(?i)\[[a-z]+://[^ \]]+ ([^\]]*)\]""".r.replaceAllIn(text, "$1") // URI 제거

      for (i <- 1 to 10)
        text = text.replaceAll( """(^|\]\])([^\(\[]*) ?\([^\(\)]*\)""", "$1$2") // 괄호 제거

      text = text.split("\n").map(_.trim).mkString("\n") // 모든 줄을 trim으로 정리함
      text = removable_sections_regex.replaceAllIn(text, "")

      text = text.replaceAll("\n[ ]*[*#:;]+[ ]*", "\n")
      text.trim
    }
    catch {
      case e: WikiPageNotFoundException => null
    }

    def getDisplayedText = new PageTextOps(getLinkedText).removeLink

    def getDisplayedSections = new PageTextOps(new PageTextOps(getLinkedText).removeLink).sections

    def getTitleText = try {
      page.getTitle.toString
    } catch {
      case e: WikiTitleParsingException => "((empty))"
      case e: Throwable => "((error))"
    }
  }

  implicit class PageTextOps(text: String) {
    def removeLink =
      text.replaceAll( """\[\[([^\|\[\]]+)\|([^\[\]]+)\]\]""", "$2").replaceAll( """\[\[([^\[\]]+)\]\]""", "$1")

    def sections(level: Int = 1): Iterable[(String, String)] = {
      val text2 = "【__TOP_SECTION__\n" + text.replaceAll(s"(?m)^=={1,$level}([^=]+)=={1,$level}$$", "】【$1\n") + "】"
      "【(.+)([^】]+)】".r.findAllMatchIn(text2).toIterable
        .map(x => (x.group(1).trim, x.group(2).replaceAll(s"(?m)^=={${level + 1},}([^=]+)=={${level + 1},}$$", "").trim))
        .filter(_._2.length > 0)
    }

    def sections: Iterable[(String, String)] = sections()
  }

  def main(args: Array[String]) {
    args.at(0, null) match {
      case "WikipediaE" => testWikipediaE()
      case "WikipediaK" => testWikipediaK()
      case "Wikidata" => testWikidata()
      case _ =>
    }
  }

  def testWikidata() = test(method, () => {
    val kb = new Wikipedia("143.248.48.105/enwiki", "admin", "admin1", "english")
    val labels = List(5, 6256, 34770, 515, 4022, 4830453, 11424) // human, country, language, city, river, business enterprise, film
    File("target/data").jfile.mkdirs()

    for (db <- new Cassandra("143.248.48.105/wikidata", "tuple2").manage()) {
      for {
        o <- labels
        o2 = db.one("SELECT label1 FROM item1 WHERE v=0 and i=?", o)
      } {
        val n = new AtomicInteger
        for {
          s <- db.ones("SELECT s FROM tuple3 WHERE v=0 and o=? and p=? limit 500", o, 31)
          os = db.ones("SELECT o FROM tuple1 WHERE v=0 and s=? and p=?", s, 31) if os.count(labels.contains) == 1
          s3 = db.one("SELECT link1 FROM item1 WHERE v=0 and i=?", s).asStr if s3.startsWith("enwiki/")
          s4 = s3.replaceFirst("^enwiki/", "").replace("''", "'")
          page = kb.getPage(s4) if page != null
          text = page.getDisplayedSections.head._2.replace("\n\n", "\n") if text.length >= 500
          n2 = n.incrementAndGet if n2 <= 100
        } {
          File(s"target/data/$o2$n2.txt").writeAll(s"$s4", Properties.lineSeparator, text)
          warn(s"[DONE] Saved $o2$n2.txt -- Q$s -- $s4")
        }
      }
    }
  })

  def testWikipediaE() = test(method, () => {
    val kb = new Wikipedia("143.248.48.105/enwiki", "admin", "admin1", "english")

    warn("  + Pages by IDs")
    for {
      i <- Seq(12, 25, 39, 290, 303)
      p = kb.getPage(i) if p != null
      (id, title, text) = (p.getPageId, p.getTitle, p.getLinkedText)
      (isDsm, isDsc, isRdr) = (p.isDisambiguation.asInt, p.isDiscussion.asInt, p.isRedirect.asInt)
      first = text.split("\n").head
    } warn(s"    - [$id] $title($isDsm/$isDsc/$isRdr) = $first")

    warn("  + Pages by titles")
    for {
      t <- "USA, UK, Korea, KAIST, Anarchist, Anarchists (disambiguation)".split(", ")
      p = kb.getPage(t) if p != null
      (id, title, text) = (p.getPageId, p.getTitle, p.getLinkedText)
      (isDsm, isDsc, isRdr) = (p.isDisambiguation.asInt, p.isDiscussion.asInt, p.isRedirect.asInt)
      first = text.split("\n").head
    } warn(s"    - [$id] $title($isDsm/$isDsc/$isRdr) = $first")
  })

  def testWikipediaK() = test(method, () => {
    val kb = new Wikipedia("143.248.48.105/kowiki", "admin", "admin1", "korean")

    warn("  + Pages by IDs")
    for {
      i <- Seq(5, 9, 10, 19, 20)
      p = kb.getPage(i) if p != null
      (id, title, text) = (p.getPageId, p.getTitle, p.getLinkedText)
      (isDsm, isDsc, isRdr) = (p.isDisambiguation.asInt, p.isDiscussion.asInt, p.isRedirect.asInt)
      first = text.split("\n").head
    } warn(s"    - [$id] $title($isDsm/$isDsc/$isRdr) = $first")

    warn("  + Pages by titles")
    for {
      t <- "초월수, 대수적 수, KAIST, 카이스트, 한국과학기술원".split(", ")
      p = kb.getPage(t) if p != null
      (id, title, text) = (p.getPageId, p.getTitle, p.getLinkedText)
      (isDsm, isDsc, isRdr) = (p.isDisambiguation.asInt, p.isDiscussion.asInt, p.isRedirect.asInt)
      first = text.split("\n").head
    } warn(s"    - [$id] $title($isDsm/$isDsc/$isRdr) = $first")
  })
}
