package edu.kaist.tmtk

import java.util.Random

import weka.classifiers.Evaluation

import scala.collection.JavaConversions.{collectionAsScalaIterable, seqAsJavaList}

package object ml {
  def main(args: Array[String]) {
    args.at(0, null) match {
      case "Weka" => testWeka()
      case _ =>
    }
  }

  def testWeka() = test(method, () => {
    val ml = new Weka("test", 4, Seq("A", "B"))
    ml.add(Array(1, 0, 0, 0), "A")
    ml.add(Array(0, 1, 0, 0), "A")
    ml.add(Array(1, 1, 0, 0), "A")
    ml.add(Array(0, 0, 1, 0), "B")
    ml.add(Array(0, 0, 0, 1), "B")
    ml.add(Array(0, 0, 1, 1), "B")
    ml.data.stratify(3)
    ml.save("target/test.arff")

    val (train, test) = (ml.data.trainCV(3, 0), ml.data.testCV(3, 0))
    val classifier = new weka.classifiers.bayes.NaiveBayes()
    classifier.buildClassifier(train)

    warn("Classification [Train Data]")
    for (instance <- train) {
      val correct = instance.classValue().toInt
      val prediction = classifier.classifyInstance(instance).toInt
      val p = classifier.distributionForInstance(instance)
      warn("  - " + Seq(correct, prediction, p(prediction)).mkString("\t"))
    }
    warn("Classification [Test Data]")
    for (instance <- test) {
      val correct = instance.classValue().toInt
      val prediction = classifier.classifyInstance(instance).toInt
      val p = classifier.distributionForInstance(instance)
      warn("  - " + Seq(correct, prediction, p(prediction)).mkString("\t"))
    }

    val eval = new Evaluation(ml.data)
    eval.evaluateModel(classifier, test)
    warn("Result Summary" + eval.toSummaryString("", false))
    warn("Detailed Accuracy" + eval.toClassDetailsString(""))
    warn("Confusion Matrix " + eval.toMatrixString(""))

    eval.crossValidateModel(classifier, ml.data, 3, new Random(1))
    warn("Result Summary" + eval.toSummaryString("", false))
    warn("Detailed Accuracy" + eval.toClassDetailsString(""))
    warn("Confusion Matrix " + eval.toMatrixString(""))
  })
}
