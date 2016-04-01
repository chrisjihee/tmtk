package edu.kaist.tmtk

import edu.emory.clir.clearnlp.dependency.{DEPNode, DEPTree}
import edu.emory.clir.clearnlp.util.arc.SRLArc
import edu.stanford.nlp.dcoref.CorefChain
import edu.stanford.nlp.dcoref.CorefCoreAnnotations.CorefChainAnnotation
import edu.stanford.nlp.international.Language
import edu.stanford.nlp.ling.CoreAnnotations.{CharacterOffsetBeginAnnotation, SentenceIndexAnnotation, SentencesAnnotation, TextAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.{CoreLabel, IndexedWord}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.semgraph.SemanticGraph
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.trees.GrammaticalRelation.{DEPENDENT, GOVERNOR, ROOT}
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import edu.stanford.nlp.trees.{GrammaticalRelation, Tree, TypedDependency}
import edu.stanford.nlp.util.CoreMap
import opennlp.tools.parser.Parse

import scala.collection.JavaConversions.{asJavaCollection, iterableAsScalaIterable, mapAsJavaMap, mapAsScalaMap}
import scala.collection.mutable
import scala.xml.{NodeSeq, XML}

package object nlp {

  implicit class AnnotationOps(x: Annotation) {
    def text: String =
      x.get(classOf[TextAnnotation])

    def tokens: Iterable[CoreLabel] =
      x.get(classOf[TokensAnnotation])

    def sentences: Iterable[CoreMap] =
      x.get(classOf[SentencesAnnotation])

    def corefs: mutable.Map[Integer, CorefChain] =
      x.get(classOf[CorefChainAnnotation])
  }

  implicit class CoreMapOps(x: CoreMap) {
    def text: String =
      x.get(classOf[TextAnnotation])

    def tokens: Iterable[CoreLabel] =
      x.get(classOf[TokensAnnotation])

    def sent_index: Int =
      x.get(classOf[SentenceIndexAnnotation])

    def char_begin: Int =
      x.get(classOf[CharacterOffsetBeginAnnotation])

    def tree: Tree =
      x.get(classOf[TreeAnnotation])

    def deps: SemanticGraph =
      x.get(classOf[CollapsedCCProcessedDependenciesAnnotation])

    def words: Iterable[String] =
      x.tokens.map(_.word)

    def tags: Iterable[String] =
      x.tokens.map(_.tag)
  }

  implicit class ParsedSentOps(x: ParsedSent) {
    def words =
      x.word.toIterable.map(_.text)

    def lemmas =
      x.morp.toIterable.map(_.lemma)

    def tags =
      x.morp.toIterable.map(_.`type`)

    def NEs =
      x.NE.toIterable.map(x => x.text + "/" + x.`type`)

    def deps =
      x.dependency.toSeq
  }

  def toTypedDependencies(nodes: Seq[ParsedDep]) =
    nodes.map(toTypedDependency(_, nodes)).filter(_ != null)

  def toTypedDependency(node: ParsedDep, nodes: Seq[ParsedDep]) = if (node.head >= 0) {
    val rel = GrammaticalRelation.valueOf(Language.Any, node.label)
    val gov = toIndexedWord(nodes(node.head))
    val dep = toIndexedWord(node)
    new TypedDependency(rel, gov, dep)
  } else null

  def toIndexedWord(node: ParsedDep) = {
    val word = new IndexedWord()
    word.setIndex(node.id)
    word.setValue(node.text)
    word.setWord(node.text)
    word.setTag("")
    word.setLemma(node.text)
    word
  }

  private val COMMON_RELs = Map("root" -> ROOT, "dep" -> DEPENDENT, "gov" -> GOVERNOR)

  def toSRLDependencies(tree: DEPTree) =
    tree.flatMap(x => x.getSemanticHeadArcList.map(y => (x, y))).map(toTypedDependency)

  def toTypedDependencies(tree: DEPTree) =
    tree.map(toTypedDependency).filter(_ != null)

  def toTypedDependency(node: DEPNode): TypedDependency = if (node.hasHead) {
    val rel = Option(GrammaticalRelation.valueOf(node.getLabel, COMMON_RELs)).getOrElse(GrammaticalRelation.valueOf(Language.English, node.getLabel))
    val gov = toIndexedWord(node.getHead)
    val dep = toIndexedWord(node)
    new TypedDependency(rel, gov, dep)
  } else null

  def toTypedDependency(tuple: (DEPNode, SRLArc)): TypedDependency = toTypedDependency(tuple._1, tuple._2)

  def toTypedDependency(node: DEPNode, sarc: SRLArc): TypedDependency = if (sarc.getNode != null) {
    val rel = GrammaticalRelation.valueOf(Language.English, sarc.getLabel)
    val gov = toIndexedWord(sarc.getNode)
    val dep = toIndexedWord(node)
    new TypedDependency(rel, gov, dep)
  } else null

  def toIndexedWord(node: DEPNode) = {
    val word = new IndexedWord()
    word.setIndex(node.getID)
    word.setValue(node.getWordForm)
    word.setWord(node.getWordForm)
    word.setTag(node.getPOSTag)
    word.setLemma(node.getLemma)
    word
  }

  def toSemanticGraph(deps: Iterable[TypedDependency], reset: Boolean = true): SemanticGraph = if (deps.nonEmpty) {
    val graph = new SemanticGraph(deps)
    if (reset)
      graph.resetRoots()
    graph
  } else null

  def toSemanticGraph(deps: Iterable[TypedDependency]): SemanticGraph = toSemanticGraph(deps, reset = true)

  def toChunkString(toks: Array[String], tags: Array[String], cnks: Array[String]) = {
    val buffer = new StringBuffer
    for (i <- cnks.indices) {
      if (i > 0 && !cnks(i).startsWith("I-") && !(cnks(i - 1) == "O")) buffer.append(">")
      if (i > 0 && cnks(i).startsWith("B-")) buffer.append(" <" + cnks(i).substring(2))
      else if (cnks(i).startsWith("B-")) buffer.append("<" + cnks(i).substring(2))
      buffer.append(s" ${toks(i)}/${tags(i)}")
    }
    if (!(cnks(cnks.length - 1) == "O")) buffer.append(">")
    buffer.toString
  }

  def toTreeString(tree: Parse) = {
    val buffer = new StringBuffer
    tree.show(buffer)
    buffer.toString
  }

  def main(args: Array[String]) {
    val textE = "Abraham Lincoln was the 16th President of the United States, serving from March 1861 until his assassination in April 1865." +
      " Lincoln led the United States through its Civil War -- its bloodiest war and its greatest moral, constitutional, and political crisis." +
      " In doing so, he preserved the Union, abolished slavery, strengthened the federal government, and modernized the economy."
    val textK = "조선 세종은 조선의 제4대 왕이다. 성은 이, 휘는 도, 본관은 전주, 자는 원정, 아명은 막동이다." +
      " 세종은 묘호이며, 시호는 영문예무인성명효대왕이고, 명에서 받은 시호는 장헌이다. 존시를 합치면 세종장헌영문예무인성명효대왕이 된다." +
      " 태종과 원경왕후의 셋째 아들이며, 비는 청천부원군 심온의 딸 소헌왕후 심씨이다."
    //val textE2 = "Samsung Electronics is a South Korean multinational electronics company in Suwon, South Korea."
    val textE3 = "John may like an ice cream cake of the shop very much."
    args.at(0, null) match {
      case "OpenNLP" => testOpenNLP(textE)
      case "StanfordNLP" => testStanfordNLP(textE)
      case "StanfordNLP2" => testStanfordNLP2()
      case "ClearNLP" => testClearNLP(textE3)
      case "HannanumNLP" => testHannanumNLP(textK)
      case "KoreanNLP" => testKoreanNLP(textK)
      case _ => test("edu.kaist.tmtk.nlp")
    }
  }

  def testOpenNLP(text: String) = test(method, () => {
    val nlp = new OpenNLP("tokenize, ssplit, pos, chunk, parse, ner")
    for (sentence <- nlp.detect(text)) {
      val tokens = nlp.tokenize(sentence)
      warn(s" + [   Tokenized] ${tokens.mkString(" ")}")
      val tags = nlp.tag(tokens)
      val postagged = tokens.zip(tags).map(x => s"${x._1}/${x._2}")
      warn(s"   - [POS-Tagged] ${postagged.mkString(" ")}")
      val recognized = nlp.recognize(tokens)
      warn(s"   - [Recognized] $recognized")
      val phchunked = nlp.chunkToString(tokens, tags)
      warn(s"   - [Ph-Chunked] $phchunked")
      val lexparsed = nlp.parseToString(tokens.mkString(" "))
      warn(s"   - [Lex-Parsed] $lexparsed")
    }
  })

  def testStanfordNLP(text: String) = test(method, () => {
    val nlp = new StanfordNLP("tokenize, ssplit, pos, lemma, ner, parse, depparse, dcoref")
    for (sentence <- nlp.analyze(text).sentences) {
      val tokens = sentence.tokens.map(_.word)
      warn(s" + [   Tokenized] ${tokens.mkString(" ")}")
      val lemmas = sentence.tokens.map(_.lemma)
      warn(s"   - [Lemmatized] ${lemmas.mkString(" ")}")
      val tags = sentence.tokens.map(_.tag)
      val postagged = tokens.zip(tags).map(x => s"${x._1}/${x._2}")
      warn(s"   - [POS-Tagged] ${postagged.mkString(" ")}")
      val ners = sentence.tokens.map(_.ner)
      val recognized = tokens.zip(ners).map(x => s"${x._1}/${x._2}")
      warn(s"   - [Recognized] ${recognized.mkString(" ")}")
      val lexparsed = sentence.tree
      warn(s"   - [Lex-Parsed] $lexparsed")
      val depparsed = sentence.deps
      val deps = depparsed.toList.split("\n")
      warn(s"   - [Dep-Parsed] ${deps.mkString(" / ")}")
      warn(s"   - [Dep-Parsed] \n${depparsed.toString.trim}")
    }
  })

  def testStanfordNLP2() = test(method, () => {
    val text = "Last summer, they met every Tuesday afternoon, from 1 pm to 3 pm. I went to school yesterday. I will come back two weeks later."
    val date = "2016-01-01"
    val nlp = StanfordNLP.apply("normalize")
    val times = XML.loadString(nlp.normalize(text, date)) \\ "TIMEX3"
    println(times.size)
    for (time <- times)
      println(time.toString())
  })

  def testClearNLP(text: String) = test(method, () => {
    val nlp = new ClearNLP("tokenize, ssplit, pos, lemma, dep, srl, ner")
    for (sentence <- nlp.analyze(text)) {
      val tokens = sentence.map(_.getWordForm)
      warn(s" + [   Tokenized] ${tokens.mkString(" ")}")
      val lemmas = sentence.map(_.getLemma)
      warn(s"   - [Lemmatized] ${lemmas.mkString(" ")}")
      val tags = sentence.map(_.getPOSTag)
      val postagged = tokens.zip(tags).map(x => s"${x._1}/${x._2}")
      warn(s"   - [POS-Tagged] ${postagged.mkString(" ")}")
      val ners = sentence.map(_.getNamedEntityTag).map(x => if (x != null) x else "X")
      val recognized = tokens.zip(ners).map(x => s"${x._1}/${x._2}")
      warn(s"   - [Recognized] ${recognized.mkString(" ")}")
      val deps = toTypedDependencies(sentence)
      val depparsed = toSemanticGraph(deps)
      warn(s"   - [Dep-Parsed] ${deps.mkString(" / ")}")
      warn(s"   - [Dep-Parsed] \n${depparsed.toString.trim}")
      val pbvs = sentence.map(_.getFeat("pb")).map(x => if (x != null) x else "X")
      val pbvtagged = tokens.zip(pbvs).map(x => s"${x._1}/${x._2}")
      warn(s"   - [PBV-Tagged] ${pbvtagged.mkString(" ")}")
      val srls = toSRLDependencies(sentence)
      val srlabeled = toSemanticGraph(srls)
      warn(s"   - [SR-Labeled] ${srls.mkString(" / ")}")
      warn(s"   - [SR-Labeled] \n${srlabeled.toString.trim}")
    }
  })

  def testHannanumNLP(text: String) = test(method, () => {
    val nlp = new HannanumNLP("ssplit, pos")
    for (sentence <- nlp.detect(text)) {
      warn(s" + [Raw Sentence] $sentence")
      warn(s"   - [POS-Tagged] ${nlp.tag(sentence).mkString(" ")}")
    }
    nlp.close()
  })

  def testKoreanNLP(text: String) = test(method, () => {
    val nlp = new KoreanNLP("143.248.48.105:30600")
    for (sentence <- nlp.analyze(text).sentences) {
      val words = sentence.words
      warn(s" + [Raw Sentence] ${words.mkString(" ")}")
      val lemmas = sentence.lemmas
      warn(s"   - [Lemmatized] ${lemmas.mkString(" ")}")
      val tags = sentence.tags
      val postagged = lemmas.zip(tags).map(x => s"${x._1}/${x._2}")
      warn(s"   - [POS-Tagged] ${postagged.mkString(" ")}")
      val recognized = sentence.NEs
      warn(s"   - [Recognized] ${recognized.mkString(" ")}")
      val deps = toTypedDependencies(sentence.deps)
      val depparsed = toSemanticGraph(deps)
      warn(s"   - [Dep-Parsed] ${deps.mkString(" / ")}")
      warn(s"   - [Dep-Parsed] \n${depparsed.toString.trim}")
    }
  })
}
