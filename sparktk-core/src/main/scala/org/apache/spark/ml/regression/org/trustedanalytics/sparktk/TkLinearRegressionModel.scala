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
package org.apache.spark.ml.regression.org.trustedanalytics.sparktk

import org.apache.spark.ml.regression.LinearRegressionModel

/**
 * The purpose of this TkLinearRegressionModel is to give us access to the protected predict() method in the Spark
 * MLLib LinearRegressionModel, which we need to use for scoring
 */
case class TkLinearRegressionModel(linearRegressionData: LinearRegressionData) extends LinearRegressionModel(linearRegressionData.model.uid, linearRegressionData.model.coefficients, linearRegressionData.model.intercept) {

  /**
   * Call through to the LinearRegressionModel predict
   * @param features Feature vector
   * @return Predict result
   */
  def vectorPredict(features: org.apache.spark.mllib.linalg.Vector): scala.Double = {
    predict(features)
  }

}

/**
 * Linear Regression data object
 * @param model The trained LinearRegression Model
 * @param observationColumns Frame's column(s) storing the observations
 * @param labelColumn Frame's column storing the label
 */
case class LinearRegressionData(model: LinearRegressionModel, observationColumns: Seq[String], labelColumn: String) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
  require(model != null, "linRegModel must not be null")
  require(labelColumn != null && labelColumn != "", "labelColumn must not be null or empty")
}
