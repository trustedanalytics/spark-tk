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

import org.trustedanalytics.sparktk.frame.internal.rdd.{ ScoreAndLabel, FrameRdd }
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait MultiClassClassificationMetricsSummarization extends BaseFrame {

  def multiClassClassificationMetrics(labelColumn: String,
                                      predColumn: String,
                                      beta: Double = 1.0,
                                      frequencyColumn: Option[String]): ClassificationMetricValue = {
    execute(MultiClassClassificationMetrics(labelColumn, predColumn, beta, frequencyColumn))
  }
}

/**
 * Model statistics of accuracy, precision, and others.
 *
 * @param labelColumn The name of the column containing the correct label for each instance.
 * @param predColumn The name of the column containing the predicted label for each instance.
 * @param beta This is the beta value to use for :math:`F_{ \beta}` measure (default F1 measure is
 *             computed); must be greater than zero. Default is 1.
 * @param frequencyColumn The name of an optional column containing the frequency of observations.
 */
case class MultiClassClassificationMetrics(labelColumn: String,
                                           predColumn: String,
                                           beta: Double,
                                           frequencyColumn: Option[String]) extends FrameSummarization[ClassificationMetricValue] {
  require(StringUtils.isNotEmpty(labelColumn), "label column is required")
  require(StringUtils.isNotEmpty(predColumn), "predict column is required")
  require(beta >= 0, "invalid beta value for f measure. Should be greater than or equal to 0")

  override def work(state: FrameState): ClassificationMetricValue = {
    ClassificationMetricsFunctions.multiclassClassificationMetrics(
      state,
      labelColumn,
      predColumn,
      beta,
      frequencyColumn
    )
  }
}

/**
 * Model Accuracy, Precision, Recall, FMeasure, Confusion matrix for multi-class
 */
class MultiClassMetrics[T: ClassTag](labelPredictRdd: RDD[ScoreAndLabel[T]],
                                     beta: Double = 1) extends Serializable {

  def this(frameRdd: FrameRdd,
           labelColumn: String,
           predictColumn: String,
           beta: Double = 1,
           frequencyColumn: Option[String] = None) {
    this(frameRdd.toScoreAndLabelRdd[T](labelColumn, predictColumn, frequencyColumn), beta)
  }

  labelPredictRdd.cache()

  lazy val countsByLabel = labelPredictRdd.map(scoreAndLabel => {
    (scoreAndLabel.label, scoreAndLabel.frequency)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val countsByScore = labelPredictRdd.map(scoreAndLabel => {
    (scoreAndLabel.score, scoreAndLabel.frequency)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val truePositivesByLabel = labelPredictRdd.map(scoreAndLabel => {
    val truePositives = if (ClassificationMetricsFunctions.compareValues(scoreAndLabel.label, scoreAndLabel.score)) {
      scoreAndLabel.frequency
    }
    else {
      0L
    }
    (scoreAndLabel.label, truePositives)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val falsePositivesByScore = labelPredictRdd.map(scoreAndLabel => {
    val falsePositives = if (!ClassificationMetricsFunctions.compareValues(scoreAndLabel.label, scoreAndLabel.score)) {
      scoreAndLabel.frequency
    }
    else {
      0L
    }
    (scoreAndLabel.score, falsePositives)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val totalCount = countsByLabel.map { case (label, count) => count }.sum

  lazy val totalTruePositives = truePositivesByLabel.map { case (label, count) => count }.sum

  /**
   * Compute precision for label
   *
   * Precision = true positives/(true positives + false positives)
   *
   * @param label Class label
   * @return precision
   */
  def precision(label: T): Double = {
    val truePos = truePositivesByLabel.getOrElse(label, 0L)
    val falsePos = falsePositivesByScore.getOrElse(label, 0L)

    (truePos + falsePos) match {
      case 0 => 0d
      case totalPos => truePos / totalPos.toDouble
    }
  }

  /**
   * Compute recall for label
   *
   * Recall = true positives/(true positives + false negatives)
   * @param label Class label
   * @return recall
   */
  def recall(label: T): Double = {
    val truePos = truePositivesByLabel.getOrElse(label, 0L)
    val labelCount = countsByLabel.getOrElse(label, 0L)

    labelCount match {
      case 0 => 0d
      case _ => truePos / labelCount.toDouble
    }
  }

  /**
   * Compute f-measure for label
   *
   * Fmeasure is weighted average of precision and recall
   * @param label Class label
   * @return F-measure
   */
  def fmeasure(label: T): Double = {
    val labelPrecision = precision(label)
    val labelRecall = recall(label)

    (math.pow(beta, 2) * labelPrecision) + labelRecall match {
      case 0 => 0
      case _ => (1 + math.pow(beta, 2)) * ((labelPrecision * labelRecall) / ((math.pow(beta, 2) * labelPrecision) + labelRecall))
    }
  }

  /**
   * Compute precision weighted by label occurrence
   */
  def weightedPrecision(): Double = {
    if (totalCount > 0) {
      countsByLabel.map {
        case (label, labelCount) =>
          labelCount * precision(label)
      }.sum / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute recall weighted by label occurrence
   */
  def weightedRecall(): Double = {
    if (totalCount > 0) {
      countsByLabel.map {
        case (label, labelCount) =>
          labelCount * recall(label)
      }.sum / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute f-measure weighted by label occurrence
   */
  def weightedFmeasure(): Double = {
    if (totalCount > 0) {
      countsByLabel.map {
        case (label, labelCount) =>
          labelCount * fmeasure(label)
      }.sum / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute model accuracy
   */
  def accuracy(): Double = {
    if (totalCount > 0) {
      totalTruePositives / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Returns string representation of the specified value
   */
  private def valueToString(value: Any): String = {
    if (value == null)
      "None"
    else
      value.toString
  }

  /**
   * Compute confusion matrix for labels
   */
  def confusionMatrix(): ConfusionMatrix = {
    val predictionSummary = labelPredictRdd.map(scoreAndLabel => {
      ((scoreAndLabel.score, scoreAndLabel.label), scoreAndLabel.frequency)
    }).reduceByKey(_ + _).collectAsMap()

    val rowLabels = predictionSummary.map { case ((score, label), frequency) => valueToString(label) }.toSet.toList.sorted
    val colLabels = predictionSummary.map { case ((score, label), frequency) => valueToString(score) }.toSet.toList.sorted
    val matrix = ConfusionMatrix(rowLabels, colLabels)
    predictionSummary.foreach {
      case ((score, label), frequency) =>
        matrix.set(valueToString(score), valueToString(label), frequency)
    }
    matrix
  }
}
