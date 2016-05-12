package edu.kaist.tmtk.ml

import java.lang.{Double => JDouble, Integer => JInt}
import java.util.{ArrayList => JArrayList}

import edu.kaist.tmtk._
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
    val data = new Instances(dataname, new JArrayList[Attribute](attrs), 0)
    data.setClassIndex(attrs.size - 1)
    data
  }

  def add(features: Array[Double], label: String) = {
    val values = features.toBuffer += data.classAttribute().indexOfValue(label).asDouble
    data.add(new DenseInstance(1.0, values.toArray))
  }

  def add(features: Array[JDouble], label: String) = {
    val values = features.toBuffer[JDouble].map(_.asDouble) += data.classAttribute().indexOfValue(label).asDouble
    data.add(new DenseInstance(1.0, values.toArray))
  }

  def add(features: Array[Int], label: String) = {
    val values = features.toBuffer[Int].map(_.asDouble) += data.classAttribute().indexOfValue(label).asDouble
    data.add(new DenseInstance(1.0, values.toArray))
  }

  def add(features: Array[JInt], label: String) = {
    val values = features.toBuffer[JInt].map(_.asDouble) += data.classAttribute().indexOfValue(label).asDouble
    data.add(new DenseInstance(1.0, values.toArray))
  }

  def save(filename: String) = DataSink.write(filename, data)
}

object Weka {
  def apply(filename: String) =
    new Weka(null, -1, null, filename)

  def apply(dataname: String, sizeFeature: Int, labels: JList[String]) =
    new Weka(dataname, sizeFeature, labels)
}
