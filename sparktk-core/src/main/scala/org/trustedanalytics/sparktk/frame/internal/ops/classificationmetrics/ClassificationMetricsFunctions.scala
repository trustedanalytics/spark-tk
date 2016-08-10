/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics

import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, ScoreAndLabel }
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

/**
 * Classification metrics
 *
 * @param fMeasure Weighted average of precision and recall
 * @param accuracy Fraction of correct predictions
 * @param recall Fraction of positives correctly predicted
 * @param precision Fraction of correct predictions among positive predictions
 * @param confusionMatrix Matrix of actual vs. predicted classes
 */
case class ClassificationMetricValue(fMeasure: Double,
                                     accuracy: Double,
                                     recall: Double,
                                     precision: Double,
                                     confusionMatrix: ConfusionMatrix)

/**
 * Model Accuracy, Precision, Recall, FMeasure, ConfusionMatrix
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 *
 * TODO: this class doesn't really belong in the Engine but it is shared code that both frame-plugins and graph-plugins need access to
 */
object ClassificationMetricsFunctions extends Serializable {

  /**
   * compute classification metrics for multi-class classifier using weighted averaging
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumn column name for the correctly labeled data
   * @param predictColumn column name for the model prediction
   * @param beta the beta value to use to compute the f measure
   * @param frequencyColumn optional column name for the frequency of each observation
   * @return a Double of the model f measure, a Double of the model accuracy, a Double of the model recall,
   *         a Double of the model precision, a map of confusion matrix values
   */
  def multiclassClassificationMetrics(frameRdd: FrameRdd,
                                      labelColumn: String,
                                      predictColumn: String,
                                      beta: Double,
                                      frequencyColumn: Option[String]): ClassificationMetricValue = {

    val multiClassMetrics = new MultiClassMetrics(frameRdd, labelColumn, predictColumn, beta, frequencyColumn)

    ClassificationMetricValue(
      multiClassMetrics.weightedFmeasure(),
      multiClassMetrics.accuracy(),
      multiClassMetrics.weightedRecall(),
      multiClassMetrics.weightedPrecision(),
      multiClassMetrics.confusionMatrix()
    )
  }

  def multiclassClassificationMetrics[T: ClassTag](labelPredictRdd: RDD[ScoreAndLabel[T]],
                                                   beta: Double = 1): ClassificationMetricValue = {

    val multiClassMetrics = new MultiClassMetrics(labelPredictRdd, beta)

    ClassificationMetricValue(
      multiClassMetrics.weightedFmeasure(),
      multiClassMetrics.accuracy(),
      multiClassMetrics.weightedRecall(),
      multiClassMetrics.weightedPrecision(),
      multiClassMetrics.confusionMatrix()
    )
  }

  /**
   * compute classification metrics for binary classifier
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumn column name for the correctly labeled data
   * @param predictColumn column name for the model prediction
   * @param positiveLabel positive label
   * @param beta the beta value to use to compute the f measure
   * @param frequencyColumn optional column name for the frequency of each observation
   * @return a Double of the model f measure, a Double of the model accuracy, a Double of the model recall,
   *         a Double of the model precision, a map of confusion matrix values
   */
  def binaryClassificationMetrics(frameRdd: FrameRdd,
                                  labelColumn: String,
                                  predictColumn: String,
                                  positiveLabel: Any,
                                  beta: Double,
                                  frequencyColumn: Option[String]): ClassificationMetricValue = {

    val binaryClassMetrics = new BinaryClassMetrics(frameRdd, labelColumn, predictColumn,
      positiveLabel, beta, frequencyColumn)

    ClassificationMetricValue(
      binaryClassMetrics.fmeasure(),
      binaryClassMetrics.accuracy(),
      binaryClassMetrics.recall(),
      binaryClassMetrics.precision(),
      binaryClassMetrics.confusionMatrix()
    )
  }

  /**
   * compute classification metrics for binary classifier
   *
   * @param positiveLabel positive label
   * @param beta the beta value to use to compute the f measure
   * @return a Double of the model f measure, a Double of the model accuracy, a Double of the model recall,
   *         a Double of the model precision, a map of confusion matrix values
   */
  def binaryClassificationMetrics[T](labelPredictRdd: RDD[ScoreAndLabel[T]],
                                     positiveLabel: Any,
                                     beta: Double = 1): ClassificationMetricValue = {

    val binaryClassMetrics = new BinaryClassMetrics(labelPredictRdd, positiveLabel, beta)

    ClassificationMetricValue(
      binaryClassMetrics.fmeasure(),
      binaryClassMetrics.accuracy(),
      binaryClassMetrics.recall(),
      binaryClassMetrics.precision(),
      binaryClassMetrics.confusionMatrix()
    )
  }

  /**
   * Compares valueA to valueB and returns true if they match
   * @param valueA First value to compare
   * @param valueB Second value to compare
   * @return True if valueA equals valueB
   */
  def compareValues(valueA: Any, valueB: Any): Boolean = {
    if (valueA != null && valueB != null)
      return valueA.equals(valueB)
    else
      return (valueA == null && valueB == null)
  }
}
