package edu.kaist.tmtk.ml

import java.io.File
import java.util
import java.util.regex.Pattern

import cc.mallet.pipe._
import cc.mallet.pipe.iterator.FileIterator
import cc.mallet.topics.ParallelTopicModel
import cc.mallet.types.{FeatureSequence, IDSorter, InstanceList}

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap

class LDA(data_dir: String) {
  var DEFAULT_NUM_KEYWORDS = 20
  var DEFAULT_ALPHA_SUM = 50.0
  var DEFAULT_BETA = 0.01

  val data = {
    val pipes = new util.ArrayList[Pipe]()
    pipes.add(new Target2Label())
    pipes.add(new SaveDataInSource())
    pipes.add(new Input2CharSequence("UTF-8"))
    pipes.add(new CharSequence2TokenSequence(Pattern.compile("\\p{Alpha}+")))
    pipes.add(new TokenSequenceLowercase())
    pipes.add(new TokenSequenceRemoveStopwords(false, false))
    pipes.add(new TokenSequence2FeatureSequence())
    val instances = new InstanceList(new SerialPipes(pipes))
    instances.addThruPipe(new FileIterator(Array(new File(data_dir)), FileIterator.STARTING_DIRECTORIES, true))
    instances
  }

  def cluster(numCluster: Int, numIteration: Int) = {
    val model = new ParallelTopicModel(numCluster, DEFAULT_ALPHA_SUM, DEFAULT_BETA)
    model.addInstances(this.data)
    model.setTopicDisplay(500, DEFAULT_NUM_KEYWORDS)
    model.setNumIterations(numIteration)
    model.setOptimizeInterval(0)
    model.setBurninPeriod(200)
    model.setSymmetricAlpha(false)
    model.setNumThreads(1)
    model.estimate()

    val topics = for (i <- 0 until numCluster) yield {
      val words = for (info <- model.getSortedWords.get(i).toList) yield
        f"${model.alphabet.lookupObject(info.getID)}/${info.getWeight}%.0f"
      (i, words)
    }
    val clusteredWords = ListMap(topics: _*)

    val output = for (id <- 0 until model.data.size) yield {
      val result = model.data.get(id)
      val name = result.instance.getName.toString
      val input = result.instance.getData.asInstanceOf[FeatureSequence]
      val topic_counts = new util.LinkedHashMap[Int, Int]
      for (i <- 0 until numCluster)
        topic_counts(i) = 0
      for (i <- result.topicSequence.getFeatures)
        topic_counts(i) = topic_counts(i) + 1
      val outputs = for (i <- 0 until numCluster) yield {
        val prob = (topic_counts(i) + model.alpha(i)) / (result.topicSequence.getFeatures.length + model.alphaSum)
        new IDSorter(i, prob)
      }
      (id, name, input, outputs.sortBy(_.getWeight * -1))
    }

    (output, clusteredWords)
  }
}
