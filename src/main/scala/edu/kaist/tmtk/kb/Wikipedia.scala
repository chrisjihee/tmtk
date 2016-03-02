package edu.kaist.tmtk.kb

import de.tudarmstadt.ukp.wikipedia.api.WikiConstants.Language
import de.tudarmstadt.ukp.wikipedia.api.exception.WikiPageNotFoundException
import de.tudarmstadt.ukp.wikipedia.api.{DatabaseConfiguration, Page}
import edu.kaist.tmtk.quite1

import scala.collection.JavaConversions.iterableAsScalaIterable

class Wikipedia(path: String, user: String, pswd: String, lang: String) {
  private val Array(host, name) = path.split("/")
  private val lang2 = Language.valueOf(lang)
  val w = new de.tudarmstadt.ukp.wikipedia.api.Wikipedia(new DatabaseConfiguration(host, name, user, pswd, lang2))
  override val toString = s"Wikipedia($lang)"

  def getPage(title: String, exactly: Boolean = false) = try {
    quite1(() => new Page(w, title, exactly))
  } catch {
    case e: WikiPageNotFoundException => null
  }

  def getPage(nid: Int) = w.getPage(nid)
}
