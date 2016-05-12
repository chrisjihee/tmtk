package edu.kaist.tmtk

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import de.tudarmstadt.ukp.wikipedia.api.Page
import de.tudarmstadt.ukp.wikipedia.api.exception.{WikiPageNotFoundException, WikiTitleParsingException}
import edu.kaist.tmtk.db.Cassandra

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
import scala.util.Properties.lineSeparator

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

    def getInfoBox: Map[String, String] = getInfobox()

    def getInfobox(type1: Boolean = false, type2: Boolean = true): Map[String, String] = try {
      val text = page.getText
      val meta1 = {
        if (type1)
          for (x <- """(?m)^\{\{.+\}\}$""".r.findAllIn(text))
            yield x.replaceAll("""^\{\{|\}\}$""", "") -> null
        else null
      }
      val meta2 = {
        if (type2)
          for (x <- """(?ms)^\{\{.+?^\}\}$""".r.findAllIn(text.replaceAll("""(?m)^\{\{.+\}\}$""", "")))
            yield {
              val h :: r = x.stripMargin('|').replaceAll("""^\{\{|\}\}$""", "").trim.split("\n").map(_.trim).toList
              h -> r.mkString("\n")
            }
        else null
      }
      if (type1 && type2)
        meta1.toMap ++ meta2.toMap
      else if (type1)
        meta1.toMap
      else if (type2)
        meta2.toMap
      else
        null
    } catch {
      case e: WikiPageNotFoundException => null
    }

    def getLinkedText: String = getLinkedText()

    def getLinkedText(title: String = null, minCh: Int = 3): String = try {
      var text = page.getText

      text = "^:.+".r.replaceAllIn(text, "") // 특별 시작문구 제거

      text = """\{\{(nihongo)\|([^|\}]+)\|([^|\}]+)[^\}]*\}\}""".r.replaceAllIn(text, "$2")
      text = """\{\{(lang)\|([^|\}]+)\|([^|\}]+)[^\}]*\}\}""".r.replaceAllIn(text, "$3")
      text = """\{\{(convert)\|([^|\}]+)\|([^|\}]+)[^\}]*\}\}""".r.replaceAllIn(text, "$2$3")

      text = text.replace("&nbsp;", " ")
      text = text.replace("&ndash;", "–")
      text = text.replace("C-change", "°C")
      text = text.replace("F-change", "°F")

      text = text.replaceAll("""\{\{.+?\}\}""", "")
      text = text.replaceAll("""(?s)\{\{.+?\}\}""", "")
      text = text.replaceAll("""(?s)<!--.+?-->""", "")
      text = text.replaceAll("(?i)(<ref>|<ref[^>/]*>).*?</ref>", "")
      text = text.replaceAll("(?is)(<ref>|<ref[^>/]*>).*?</ref>", "")
      text = text.replaceAll("(?i)<ref[^>/]*/>", "")

      text = text.replaceAll("""\{.+?\}""", "")
      text = text.replaceAll("""(?s)\{.+?\}""", "")

      text = text.replaceAll("(?i)(<gallery>|<gallery[^>/]*>).+?</gallery>", "")
      text = text.replaceAll("(?is)(<gallery>|<gallery[^>/]*>).+?</gallery>", "")
      text = text.replaceAll("(?i)<gallery[^>/]*/>", "")

      text = text.replaceAll("(?i)<br[^>/]*/?>?", " ")

      text = """(?i)\[\[(File|Image|파일|이미지|그림):[^\[\]]*((\[\[[^\[\]]*\]\][^\[\]]*)|(\[[^\[\]]*\][^\[\]]*))*\]\]""".r.replaceAllIn(text, "") // 파일, 이미지 제거
      text = """(?i)\[\[(Category|분류):.*\]\]""".r.replaceAllIn(text, "") // 카테고리 제거
      text = """(?i)\[[a-z]+://[^ \]]+\]\.?""".r.replaceAllIn(text, "") // URI 제거
      text = """(?i)\[[a-z]+://[^ \]]+ ([^\]]*)\]""".r.replaceAllIn(text, "$1") // URI 제거

      for (i <- 1 to 10)
        text = text.replaceAll("""(^|\]\])([^\[]*?) ?\([^z\(\)]*\)""", "$1$2") // 괄호 제거

      text = removable_sections_regex.replaceAllIn(text, "")
      text = text.replaceAll("\n[ ]*[*#:;]+[ ]*", "\n").trim
      if (title != null)
        text = text.replaceFirst("'''.+?'''", s"[[$title|${title.replaceAll(""" ?\([^\(\)]*\)""", "")}]]".replace("$", "\\$"))
      text = text.replace("'''", "")
      text = text.replace("''", "")

      text = text.replaceAll("[　  \u200B]", " ")
      text = text.split("\n").map(_.trim).mkString("\n") // 모든 줄을 trim으로 정리함
      text = text.replaceAll("\\s*[\n]\\s*", "\n").replaceAll("[\n]+", "\n")
      text = text.trim.split("\n").filter(_.length >= minCh).mkString("\n")
      text
    } catch {
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
      text.replaceAll("""\[\[([^\|\[\]]+)\|([^\[\]]+)\]\]""", "$2").replaceAll("""\[\[([^\[\]]+)\]\]""", "$1")

    def sections(level: Int = 1): Iterable[(String, String)] = {
      val text2 = "【__TOP_SECTION__\n" + text.replaceAll(s"(?m)^=={1,$level}([^=]+)=={1,$level}$$", "】【$1\n") + "】"
      "【(.+)([^】]+)】".r.findAllMatchIn(text2).toIterable
        .map(x => (x.group(1).trim, x.group(2).replaceAll(s"(?m)^=={${level + 1},}([^=]+)=={${level + 1},}$$\n", "").trim))
        .filter(_._2.length > 0)
    }

    def sections: Iterable[(String, String)] = sections()

    def firstSubjectK(title: String) = {
      val subject1 = s"""${Pattern.quote(title)}(는|은)""".r.findFirstIn(text.removeLink)
      subject1.getOrElse(title + "는")
    }
  }

  implicit class WikidataOps(db: Cassandra) {
    val kowiki = new Wikipedia("chrisjihee:jiheeryu@143.248.48.153/kowiki", "korean")
    private val koredirect = """#넘겨주기 \[\[(.+)\]\]""".r

    def idByTitleE(title: String) = db.one("SELECT i FROM item4 WHERE v=0 and link1=?", s"enwiki/$title").asInt

    def idByTitleK(title: String) = {
      var id = db.one("SELECT i FROM item5 WHERE v=0 and link2=?", s"kowiki/$title").asInt
      if (id < 0) {
        val redirect = Option(kowiki.getPage(title)).map(_.getText).flatMap(koredirect.findFirstIn).orNull
        if (redirect != null) {
          val koredirect(titleR) = redirect
          id = db.one("SELECT i FROM item5 WHERE v=0 and link2=?", s"kowiki/$titleR").asInt
        }
      }
      id
    }

    def idByLabelE(label: String) = db.ones("SELECT i FROM item2 WHERE v=0 and label1=?", label).map(_.asInt)

    def idByLabelK(label: String) = db.ones("SELECT i FROM item3 WHERE v=0 and label2=?", label).map(_.asInt)

    def labelE(id: String): String = labelE(id.drop(1).asInt)

    def labelE(id: Int) = db.one("SELECT label1 FROM item1 WHERE v=0 and i=?", id).asStr

    def labelK(id: String): String = labelK(id.drop(1).asInt)

    def labelK(id: Int) = db.one("SELECT label2 FROM item1 WHERE v=0 and i=?", id).asStr

    def titleE(id: String): String = titleE(id.drop(1).asInt)

    def titleE(id: Int) = db.one("SELECT link1 FROM item1 WHERE v=0 and i=?", id).asStr.replaceFirst("^enwiki/", "")

    def titleK(id: String): String = titleK(id.drop(1).asInt)

    def titleK(id: Int) = db.one("SELECT link2 FROM item1 WHERE v=0 and i=?", id).asStr.replaceFirst("^kowiki/", "")

    def whatHuman(id: Int) =
      if (db.one("SELECT o FROM tuple1 WHERE v=0 and s=? and p=31", id).asInt == 5) {
        db.one("SELECT o FROM tuple1 WHERE v=0 and s=? and p=21", id).asInt match {
          case 6581097 => "male"
          case 6581072 => "female"
          case x if x >= 0 => "other"
          case _ => null
        }
      } else null

    val rxLink = """\[\[([^\|\[\]]+)\|?([^\[\]]+)?\]\]""".r

    def toLinkMap(text: String, title: String = null, id: String => Int): Map[String, String] = {
      val links = for (x <- rxLink.findAllIn(text)) yield {
        val (target, mention) = x.replaceAll("""^\[\[|\]\]$""", "").split("\\|+", 2) match {
          case Array(a, b) => (a, b)
          case Array(a) => (a, a)
          case _ => error(s"[Exception] Exceptional link form: <text>$text</text>"); sys.exit(1)
        }
        Option(id(target)).filter(_ > 0).map(i => mention -> s"Q$i").orNull
      }
      (if (title != null)
        ArrayBuffer(Option(id(title)).filter(_ > 0).map(i => title -> s"Q$i").orNull) ++ links
      else
        links).filter(_ != null).toMap
    }

    def toLinkMapE(text: String, title: String = null) = toLinkMap(text, title, idByTitleE)

    def toLinkMapK(text: String, title: String = null) = toLinkMap(text, title, idByTitleK)
  }

  def main(args: Array[String]) {
    args.at(0, null) match {
      case "WikipediaE" => testWikipediaE()
      case "WikipediaK" => testWikipediaK()
      case "Wikidata" => testWikidata()
      case "Wikipedia+Wikidata" => testWikipediaWikidata()
      case _ => test("edu.kaist.tmtk.kb")
    }
  }

  def testWikipediaE() = test(method, () => {
    val kb = new Wikipedia("chrisjihee:jiheeryu@143.248.48.105/enwiki", "english")

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
    val kb = new Wikipedia("chrisjihee:jiheeryu@143.248.48.105/kowiki", "korean")

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

  def testWikidata() = test(method, () => {
    for (db <- new Cassandra("143.248.48.105/wikidata").manage()) {
      warn(s"  + idByTitleE : ${db.idByTitleE("Abraham Lincoln")}")
      warn(s"  + idByTitleK : ${db.idByTitleK("조지 워싱턴")}")
    }
  })

  def testWikipediaWikidata() = test(method, () => {
    val kb = new Wikipedia("chrisjihee:jiheeryu@143.248.48.105/enwiki", "english")
    val labels = List(5, 6256, 34770, 515, 4022, 4830453, 11424) // human, country, language, city, river, business enterprise, film
    File("target/data").jfile.mkdirs()

    for (db <- new Cassandra("143.248.48.105/wikidata").manage()) {
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
          File(s"target/data/$o2$n2.txt").writeAll(s"$s4", lineSeparator, text)
          warn(s"[DONE] Saved $o2$n2.txt -- Q$s -- $s4")
        }
      }
    }
  })
}
