package org.trustedanalytics.sparktk.frame.internal.rdd

import breeze.linalg.{ DenseVector => BDV }
import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{ Vectors, Vector, DenseVector }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.MllibAliases
import org.apache.spark.mllib.regression.LabeledPoint
import MllibAliases.{ NumericParser, parseNumeric }
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.internal.RowWrapper

import scala.beans.BeanInfo
import scala.xml.NodeSeq

class RowWrapperFunctions(self: RowWrapper) {

  /**
   * Convert Row into MLlib Dense Vector
   */
  def valuesAsDenseVector(columnNames: Seq[String]): DenseVector = {
    val array = self.valuesAsDoubleArray(columnNames)
    new DenseVector(array)
  }

  /**
   * Convert Row into MLlib Dense Vector
   */
  def valuesAsBreezeDenseVector(columnNames: Seq[String]): BDV[Double] = {
    val array = self.valuesAsDoubleArray(columnNames)
    new BDV[Double](array)
  }

  /**
   * Convert Row into MLlib Dense Vector
   */
  def valueAsNodeSeq(columnName: String, nodeName: String): NodeSeq = {
    self.nodeSeqValue(columnName, nodeName)
  }

  /**
   * Convert Row into LabeledPoint format required by MLLib
   */
  def valuesAsLabeledPoint(featureColumnNames: Seq[String], labelColumnName: String): LabeledPoint = {
    val label = DataTypes.toDouble(self.value(labelColumnName))
    val vector = valuesAsDenseVector(featureColumnNames)
    new LabeledPoint(label, vector)
  }

  /**
   * Convert Row into LabeledPointWithFrequency format required for updates in MLLib code
   */
  def valuesAsLabeledPointWithFrequency(labelColumnName: String,
                                        featureColumnNames: Seq[String],
                                        frequencyColumnName: Option[String]): LabeledPointWithFrequency = {
    val label = DataTypes.toDouble(self.value(labelColumnName))
    val vector = valuesAsDenseVector(featureColumnNames)

    val frequency = frequencyColumnName match {
      case Some(freqColumn) => DataTypes.toDouble(self.value(freqColumn))
      case _ => 1d
    }
    new LabeledPointWithFrequency(label, vector, frequency)
  }
}
/**
 * Class that represents the features and labels of a data point.
 *
 * Extension of MlLib's labeled points that supports a frequency column.
 * The frequency column contains the frequency of occurrence of each observation.
 *
 * @see org.apache.spark.mllib.regression.LabeledPoint
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@BeanInfo
case class LabeledPointWithFrequency(label: Double, features: Vector, frequency: Double) {
  override def toString: String = {
    s"($label,$features,$frequency)"
  }
}

/**
 * Parser for [[LabeledPointWithFrequency]].
 */
object LabeledPointWithFrequency {
  /**
   * Parses a string resulted from `LabeledPointWithFrequency#toString` into
   * an [[LabeledPointWithFrequency]].
   */
  def parse(s: String): LabeledPointWithFrequency = {
    if (s.startsWith("(")) {
      NumericParser.parse(s) match {
        case Seq(label: Double, numeric: Any, frequency: Double) =>
          LabeledPointWithFrequency(label, parseNumeric(numeric), frequency)
        case other =>
          throw new SparkException(s"Cannot parse $other.")
      }
    }
    else { // dense format used before v1.0
      val parts = s.split(',')
      val label = java.lang.Double.parseDouble(parts(0))
      val features = Vectors.dense(parts(1).trim().split(' ').map(java.lang.Double.parseDouble))
      val frequency = java.lang.Double.parseDouble(parts(2))
      LabeledPointWithFrequency(label, features, frequency)
    }
  }
}
