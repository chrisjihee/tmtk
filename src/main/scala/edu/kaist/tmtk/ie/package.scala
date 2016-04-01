package edu.kaist.tmtk

import edu.kaist.tmtk.nlp.OpenNLP

package object ie {
  def main(args: Array[String]) {
    args.at(0, null) match {
      case "OpenIE" => testOpenIE()
      case _ => test("edu.kaist.tmtk.ie")
    }
  }

  private val text = "Abraham Lincoln was the 16th President of the United States, serving from March 1861 until his assassination in April 1865." +
    " Lincoln led the United States through its Civil War -- its bloodiest war and its greatest moral, constitutional, and political crisis." +
    " In doing so, he preserved the Union, abolished slavery, strengthened the federal government, and modernized the economy." +
    " If I slept past noon, I'd be late for work. Early astronomers believe that the earth is the center of the universe."

  def testOpenIE() = test(method, () => {
    val nlp = new OpenNLP("ssplit", "D")
    val ie = new OpenIE
    for (sentence <- nlp.detect(text)) {
      warn(s" + [Raw Sentence] $sentence")
      for (extraction <- ie.extract(sentence))
        warn(s"   - [Extraction] $extraction")
    }
  })
}
