package edu.kaist.tmtk.kb

import de.tudarmstadt.ukp.wikipedia.api.WikiConstants.Language
import de.tudarmstadt.ukp.wikipedia.api.exception.{WikiPageNotFoundException, WikiTitleParsingException}
import de.tudarmstadt.ukp.wikipedia.api.{DatabaseConfiguration, Page}
import edu.kaist.tmtk.quite1

class Wikipedia(path: String, lang: String) {
  private val Array(user0, path2) = path.split("@")
  private val Array(user, pswd) = user0.split(":")
  private val Array(host, name) = path2.split("/")
  private val lang2 = Language.valueOf(lang)
  val w = new de.tudarmstadt.ukp.wikipedia.api.Wikipedia(new DatabaseConfiguration(host, name, user, pswd, lang2))
  override val toString = s"Wikipedia($lang)"

  def getPage(title: String, exactly: Boolean = false) = try {
    quite1(() => new Page(w, title, exactly))
  } catch {
    case _: WikiPageNotFoundException => null
    case _: WikiTitleParsingException => null
  }

  def getPage(nid: Int) = w.getPage(nid)
}
