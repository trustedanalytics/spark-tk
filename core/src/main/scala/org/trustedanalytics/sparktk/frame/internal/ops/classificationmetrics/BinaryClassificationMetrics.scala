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

import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.internal.rdd.{ FrameRdd, ScoreAndLabel }
import org.apache.spark.rdd.RDD

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait BinaryClassificationMetricsSummarization extends BaseFrame {

  def binaryClassificationMetrics(labelColumn: String,
                                  predColumn: String,
                                  posLabel: Any,
                                  beta: Double = 1.0,
                                  frequencyColumn: Option[String]): ClassificationMetricValue = {
    execute(BinaryClassificationMetrics(labelColumn, predColumn, posLabel, beta, frequencyColumn))
  }
}

/**
 * Statistics of accuracy, precision, and others for a binary classification model.
 *
 * @param labelColumn The name of the column containing the correct label for each instance.
 * @param predColumn The name of the column containing the predicted label for each instance.
 * @param posLabel The value to be interpreted as a positive instance for binary classification.
 * @param beta This is the beta value to use for :math:`F_{ \beta}` measure (default F1 measure is
 *             computed); must be greater than zero. Default is 1.
 * @param frequencyColumn The name of an optional column containing the frequency of observations.
 */
case class BinaryClassificationMetrics(labelColumn: String,
                                       predColumn: String,
                                       posLabel: Any,
                                       beta: Double,
                                       frequencyColumn: Option[String]) extends FrameSummarization[ClassificationMetricValue] {
  require(StringUtils.isNotEmpty(labelColumn), "label column is required")
  require(StringUtils.isNotEmpty(predColumn), "predict column is required")
  require(beta >= 0, "invalid beta value for f measure. Should be greater than or equal to 0")

  override def work(state: FrameState): ClassificationMetricValue = {
    ClassificationMetricsFunctions.binaryClassificationMetrics(
      state,
      labelColumn,
      predColumn,
      posLabel,
      beta,
      frequencyColumn)
  }
}

/**
 * Model Accuracy, Precision, Recall, FMeasure, Confusion matrix for binary-class
 */
case class BinaryClassCounter(var truePositives: Long = 0,
                              var falsePositives: Long = 0,
                              var trueNegatives: Long = 0,
                              var falseNegatives: Long = 0,
                              var count: Long = 0) {
  def +(that: BinaryClassCounter): BinaryClassCounter = {
    this.truePositives += that.truePositives
    this.falsePositives += that.falsePositives
    this.trueNegatives += that.trueNegatives
    this.falseNegatives += that.falseNegatives
    this.count += that.count
    this
  }
}

/**
 * Class used for calculating binary classification metrics
 * @param labelPredictRdd RDD of scores, labels, and associated frequency
 * @param positiveLabel The value to be interpreted as a positive instance for binary classification.
 * @param beta This is the beta value to use for :math:`F_{ \beta}` measure (default F1 measure is
 *             computed); must be greater than zero. Default is 1.
 */
case class BinaryClassMetrics[T](labelPredictRdd: RDD[ScoreAndLabel[T]],
                                 positiveLabel: Any,
                                 beta: Double = 1) extends Serializable {

  def this(frameRdd: FrameRdd,
           labelColumn: String,
           predictColumn: String,
           positiveLabel1: Any,
           beta: Double = 1,
           frequencyColumn: Option[String] = None) {

    this(frameRdd.toScoreAndLabelRdd[T](labelColumn, predictColumn, frequencyColumn), positiveLabel1, beta)
  }

  lazy val counter: BinaryClassCounter = labelPredictRdd.map(scoreAndLabel => {
    val counter = BinaryClassCounter()
    val score = scoreAndLabel.score
    val label = scoreAndLabel.label
    val frequency = scoreAndLabel.frequency
    counter.count += frequency

    if (ClassificationMetricsFunctions.compareValues(label, positiveLabel)
      && ClassificationMetricsFunctions.compareValues(score, positiveLabel)) {
      counter.truePositives += frequency
    }
    else if (!ClassificationMetricsFunctions.compareValues(label, positiveLabel) &&
      !ClassificationMetricsFunctions.compareValues(score, positiveLabel)) {
      counter.trueNegatives += frequency
    }
    else if (!ClassificationMetricsFunctions.compareValues(label, positiveLabel) &&
      ClassificationMetricsFunctions.compareValues(score, positiveLabel)) {
      counter.falsePositives += frequency
    }
    else if (ClassificationMetricsFunctions.compareValues(label, positiveLabel)
      && !ClassificationMetricsFunctions.compareValues(score, positiveLabel)) {
      counter.falseNegatives += frequency
    }

    counter
  }).reduce((counter1, counter2) => counter1 + counter2)

  lazy val count = counter.count
  lazy val truePositives = counter.truePositives
  lazy val trueNegatives = counter.trueNegatives
  lazy val falsePositives = counter.falsePositives
  lazy val falseNegatives = counter.falseNegatives

  /**
   * Compute precision = true positives/(true positives + false positives)
   */
  def precision(): Double = {
    truePositives + falsePositives match {
      case 0 => 0
      case _ => truePositives / (truePositives + falsePositives).toDouble
    }
  }

  /**
   * Compute recall = true positives/(true positives + false negatives)
   */
  def recall(): Double = {
    truePositives + falseNegatives match {
      case 0 => 0
      case _ => truePositives / (truePositives + falseNegatives).toDouble
    }
  }

  /**
   * Compute f-measure = weighted average of precision and recall
   */
  def fmeasure(): Double = {
    math.pow(beta, 2) * precision + recall match {
      case 0 => 0
      case _ => (1 + math.pow(beta, 2)) * ((precision * recall) / ((math.pow(beta, 2) * precision) + recall))
    }
  }

  /**
   * Compute model accuracy
   */
  def accuracy(): Double = {
    count match {
      case 0 => 0
      case total => (truePositives + trueNegatives) / total.toDouble
    }
  }

  /**
   * Compute confusion matrix
   */
  def confusionMatrix(): ConfusionMatrix = {
    val rowLabels = Array("pos", "neg")
    val colLabels = Array("pos", "neg")

    val matrix = ConfusionMatrix(rowLabels, colLabels)
    matrix.set("pos", "pos", truePositives)
    matrix.set("pos", "neg", falsePositives)
    matrix.set("neg", "pos", falseNegatives)
    matrix.set("neg", "neg", trueNegatives)
    matrix
  }
}