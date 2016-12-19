/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.sparktk.models.regression

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.trustedanalytics.sparktk.frame.DataTypes

/**
 * Return value from Linear Regression test
 * @param explainedVarianceScore The explained variance score whose best possible value is 1.0,
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param r2 The coefficient of determination
 * @param rootMeanSquaredError The square root of the mean squared error
 */
case class RegressionTestMetrics(explainedVarianceScore: Double,
                                 meanAbsoluteError: Double,
                                 meanSquaredError: Double,
                                 r2: Double,
                                 rootMeanSquaredError: Double)

/**
 * Utility functions for regression models
 */
object RegressionUtils extends Serializable {

  val predictionColumn = "predicted_value"
  val featuresName = "features"

  /**
   * Get regression metrics using trained model
   * @param predictFrame Frame with predicted and labeled data
   * @param predictionColumn Name of prediction column
   * @param valueColumn Name of value column
   * @return Regression metrics
   */
  def getRegressionMetrics(predictFrame: DataFrame,
                           predictionColumn: String,
                           valueColumn: String): RegressionTestMetrics = {
    val predictionAndValueRdd = predictFrame.select(predictionColumn, valueColumn).map(row => {
      val prediction = DataTypes.toDouble(row.get(0))
      val value = DataTypes.toDouble(row.get(1))
      (prediction, value)
    })

    val metrics = new RegressionMetrics(predictionAndValueRdd)
    val explainedVarianceScore = getExplainedVarianceScore(predictionAndValueRdd)
    RegressionTestMetrics(
      explainedVarianceScore,
      metrics.meanAbsoluteError,
      metrics.meanSquaredError,
      metrics.r2,
      metrics.rootMeanSquaredError
    )
  }

  /**
   * Get explained variance regression score
   * .
   * Explained variance score = 1 - (variance(label-prediction)/variance(label))
   * The best possible score is 1.
   * @param predictFrame Frame with predicted and labeled data
   * @param predictionColumn Name of prediction column
   * @param valueColumn Name of value column
   * @return Explained variance score
   */
  def getExplainedVarianceScore(predictFrame: DataFrame,
                                predictionColumn: String,
                                valueColumn: String): Double = {
    val predictionAndValueRdd = predictFrame.select(predictionColumn, valueColumn).map(row => {
      val prediction = DataTypes.toDouble(row.get(0))
      val value = DataTypes.toDouble(row.get(1))
      (prediction, value)
    })
    getExplainedVarianceScore(predictionAndValueRdd)
  }

  /**
   * Get explained variance regression score
   * .
   * Explained variance score = 1 - (variance(label-prediction)/variance(label))
   * The best possible score is 1.
   * @param predictionAndLabelRdd RDD of predicted and label values
   * @return Explained variance score
   */
  def getExplainedVarianceScore(predictionAndLabelRdd: RDD[(Double, Double)]): Double = {
    val summary = predictionAndLabelRdd.aggregate(new MultivariateOnlineSummarizer())(
      (summary, predLabel) => {
        val (prediction, label) = predLabel
        summary.add(Vectors.dense(label, label - prediction))
      },
      (sum1, sum2) => sum1.merge(sum2))
    val variance = summary.variance
    val score = 1d - (variance(1) / variance(0))
    score
  }
}
