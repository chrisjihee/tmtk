package edu.kaist.tmtk.ml

import java.util.{ArrayList => AList, List => JList}

import edu.kaist.tmtk.AsValue
import weka.core.converters.ConverterUtils.{DataSink, DataSource}
import weka.core.{Attribute, DenseInstance, Instances}

import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ArrayBuffer

class Weka(dataname: String, sizeFeature: Int, labels: JList[String], filename: String = null) {
  val data = if (filename != null) {
    new DataSource(filename).getDataSet
  } else {
    val attrs = new ArrayBuffer[Attribute]
    attrs ++= (1 to sizeFeature).map(x => new Attribute(x.asStr))
    attrs += new Attribute("label", labels)
    val data = new Instances(dataname, new AList[Attribute](attrs), 0)
    data.setClassIndex(attrs.size - 1)
    data
  }

  def add(features: Array[Double], label: String) = {
    val values = features.toBuffer += data.classAttribute().indexOfValue(label).asDouble
    data.add(new DenseInstance(1.0, values.toArray))
    //    val values =
    //      for (i <- attrs.indices) yield
    //        if (i < attrs.size - 1) features(i)
    //        else attrs(attrs.size - 1).indexOfValue(label)
    //    data.add(new DenseInstance(1.0, values.toArray))
  }

  def save(filename: String) = DataSink.write(filename, data)
}

object Weka {
  def apply(filename: String) =
    new Weka(null, -1, null, filename)

  def apply(dataname: String, sizeFeature: Int, labels: JList[String]) =
    new Weka(dataname, sizeFeature, labels)
}