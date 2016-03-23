package edu.kaist.tmtk.ml

import java.io.{File, FileReader}
import java.util.regex.Pattern

import cc.mallet.fst.SimpleTagger.SimpleTaggerSentence2FeatureVectorSequence
import cc.mallet.fst.{CRF, CRFTrainerByLabelLikelihood, MaxLatticeDefault, Transducer}
import cc.mallet.optimize.OptimizationException
import cc.mallet.pipe.Pipe
import cc.mallet.pipe.iterator.{LineGroupIterator, StringArrayIterator}
import cc.mallet.types.{FeatureVectorSequence, InstanceList}

import scala.collection.JavaConversions._

class MalletCRF {
  var DEFAULT_PRIOR_VARIANCE = 10.0
  var DEFAULT_LABEL = "O"
  var DEFAULT_TARGET_PROCESSING = true

  var train_data: InstanceList = null
  var test_data: InstanceList = null
  var model: CRF = null

  def setTrainData(data_file: String) = {
    val p = new SimpleTaggerSentence2FeatureVectorSequence
    p.setTargetProcessing(DEFAULT_TARGET_PROCESSING)
    train_data = new InstanceList(p)
    train_data.addThruPipe(new LineGroupIterator(new FileReader(new File(data_file)), Pattern.compile("^\\s*$"), true))
  }

  def setTrainData(data: Array[String]) = {
    val p = new SimpleTaggerSentence2FeatureVectorSequence
    p.setTargetProcessing(DEFAULT_TARGET_PROCESSING)
    train_data = new InstanceList(p)
    train_data.addThruPipe(new StringArrayIterator(data))
  }

  def setTestData(data_file: String) = {
    val p = new SimpleTaggerSentence2FeatureVectorSequence
    p.setTargetProcessing(DEFAULT_TARGET_PROCESSING)
    test_data = new InstanceList(p)
    test_data.addThruPipe(new LineGroupIterator(new FileReader(new File(data_file)), Pattern.compile("^\\s*$"), true))
  }

  def setTestData(data: Array[String]) = {
    val p = new SimpleTaggerSentence2FeatureVectorSequence
    p.setTargetProcessing(DEFAULT_TARGET_PROCESSING)
    test_data = new InstanceList(p)
    test_data.addThruPipe(new StringArrayIterator(data))
  }

  def train(numIteration: Int) = {
    model = new CRF(train_data.getPipe, null.asInstanceOf[Pipe])
    for (i <- 0 until model.numStates)
      model.getState(i).setInitialWeight(Transducer.IMPOSSIBLE_WEIGHT)
    val startName = model.addOrderNStates(train_data, Array(1), null, DEFAULT_LABEL, Pattern.compile("\\s"), Pattern.compile(".*"), true)
    model.getState(startName).setInitialWeight(0.0)

    val crft = new CRFTrainerByLabelLikelihood(model)
    crft.setGaussianPriorVariance(DEFAULT_PRIOR_VARIANCE)
    crft.setUseSparseWeights(true)
    crft.setUseSomeUnsupportedTrick(true)

    crft.train(train_data, numIteration)
  }

  def test(numOutput: Int) = {
    bufferAsJavaList(for (instance <- test_data) yield {
      val input = instance.getData.asInstanceOf[FeatureVectorSequence]
      val seqs = new MaxLatticeDefault(model, input, null, 100000).bestOutputSequences(numOutput)
      if (seqs.exists(seq => seq.size != input.size))
        System.err.println("[ERROR] Error output at " + input)
      val outputs = seqAsJavaList(for (seq <- seqs) yield
        seqAsJavaList(for (i <- 0 until seq.size) yield
          seq.get(i).toString))
      (input, outputs)
    })
  }
}