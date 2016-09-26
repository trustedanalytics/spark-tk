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
