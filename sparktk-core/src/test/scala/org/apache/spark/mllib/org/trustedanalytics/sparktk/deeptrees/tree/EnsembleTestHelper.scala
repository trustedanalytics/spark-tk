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
package org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.model.TreeEnsembleModel
import org.apache.spark.util.StatCounter

import scala.collection.mutable

object EnsembleTestHelper {

  /**
   * Aggregates all values in data, and tests whether the empirical mean and stddev are within
   * epsilon of the expected values.
   * @param data  Every element of the data should be an i.i.d. sample from some distribution.
   */
  def testRandomArrays(
    data: Array[Array[Double]],
    numCols: Int,
    expectedMean: Double,
    expectedStddev: Double,
    epsilon: Double) {
    val values = new mutable.ArrayBuffer[Double]()
    data.foreach { row =>
      assert(row.size == numCols)
      values ++= row
    }
    val stats = new StatCounter(values)
    assert(math.abs(stats.mean - expectedMean) < epsilon)
    assert(math.abs(stats.stdev - expectedStddev) < epsilon)
  }

  def validateClassifier(
    model: TreeEnsembleModel,
    input: Seq[LabeledPoint],
    requiredAccuracy: Double) {
    val predictions = input.map(x => model.predict(x.features))
    val numOffPredictions = predictions.zip(input).count {
      case (prediction, expected) =>
        prediction != expected.label
    }
    val accuracy = (input.length - numOffPredictions).toDouble / input.length
    assert(accuracy >= requiredAccuracy,
      s"validateClassifier calculated accuracy $accuracy but required $requiredAccuracy.")
  }

  /**
   * Validates a tree ensemble model for regression.
   */
  def validateRegressor(
    model: TreeEnsembleModel,
    input: Seq[LabeledPoint],
    required: Double,
    metricName: String = "mse") {
    val predictions = input.map(x => model.predict(x.features))
    val errors = predictions.zip(input).map {
      case (prediction, point) =>
        point.label - prediction
    }
    val metric = metricName match {
      case "mse" =>
        errors.map(err => err * err).sum / errors.size
      case "mae" =>
        errors.map(math.abs).sum / errors.size
    }

    assert(metric <= required,
      s"validateRegressor calculated $metricName $metric but required $required.")
  }

  def generateOrderedLabeledPoints(numFeatures: Int, numInstances: Int): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](numInstances)
    for (i <- 0 until numInstances) {
      val label = if (i < numInstances / 10) {
        0.0
      }
      else if (i < numInstances / 2) {
        1.0
      }
      else if (i < numInstances * 0.9) {
        0.0
      }
      else {
        1.0
      }
      val features = Array.fill[Double](numFeatures)(i.toDouble)
      arr(i) = new LabeledPoint(label, Vectors.dense(features))
    }
    arr
  }

}
