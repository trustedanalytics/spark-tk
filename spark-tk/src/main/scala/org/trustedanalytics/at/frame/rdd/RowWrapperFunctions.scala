package org.trustedanalytics.at.frame.rdd

import breeze.linalg.{ DenseVector => BDV }
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.{ LabeledPointWithFrequency, LabeledPoint }
import org.trustedanalytics.at.frame.RowWrapper
import org.trustedanalytics.at.frame.schema.DataTypes

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

