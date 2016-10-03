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
package org.trustedanalytics.sparktk.models.regression.linear_regression

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.spark.ml.regression.{ LinearRegressionModel => SparkLinearRegressionModel }
import scala.collection.mutable.ListBuffer
import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.apache.commons.lang.StringUtils

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object LinearRegressionModel extends TkSaveableObject {

  val predictionColumn = "predicted_value"
  val featuresName = "features"

  /**
   * Run Spark Ml's LinearRegression on the training frame and create a Model for it.
   *
   * @param frame A frame to train the model on
   * @param valueColumn Column name containing the value for each observation.
   * @param observationColumns List of column(s) containing the observations.
   * @param elasticNetParameter Parameter for the ElasticNet mixing. Default is 0.0
   * @param fitIntercept Parameter for whether to fit an intercept term. Default is true
   * @param maxIterations Parameter for maximum number of iterations. Default is 100
   * @param regParam Parameter for regularization. Default is 0.0
   * @param standardization Parameter for whether to standardize the training features before fitting the model. Default is true
   * @param tolerance Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
   * @return returns LinearRegressionModel
   */
  def train(frame: Frame,
            valueColumn: String,
            observationColumns: Seq[String],
            elasticNetParameter: Double = 0.0,
            fitIntercept: Boolean = true,
            maxIterations: Int = 100,
            regParam: Double = 0.0,
            standardization: Boolean = true,
            tolerance: Double = 1E-6): LinearRegressionModel = {

    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(valueColumn), "valueColumn must not be null nor empty")
    require(maxIterations > 0, "numIterations must be a positive value")
    require(regParam >= 0, "regParam should be greater than or equal to 0")

    val trainFrame = new FrameRdd(frame.schema, frame.rdd).toDataFrame
    val trainVectors = new VectorAssembler().setInputCols(observationColumns.toArray).setOutputCol(featuresName)

    val dataFrame = trainVectors.transform(trainFrame)

    val linReg = new LinearRegression()
    linReg.setElasticNetParam(elasticNetParameter)
      .setFitIntercept(fitIntercept)
      .setMaxIter(maxIterations)
      .setRegParam(regParam)
      .setStandardization(standardization)
      .setTol(tolerance)
      .setLabelCol(valueColumn)
      .setFeaturesCol(featuresName)

    val linRegModel = linReg.fit(dataFrame)

    linRegModel.setPredictionCol(predictionColumn)

    LinearRegressionModel(valueColumn,
      observationColumns,
      linRegModel.intercept,
      linRegModel.coefficients.toArray.toSeq,
      linRegModel.summary.explainedVariance,
      linRegModel.summary.meanAbsoluteError,
      linRegModel.summary.meanSquaredError,
      linRegModel.summary.objectiveHistory.toSeq,
      linRegModel.summary.r2,
      linRegModel.summary.rootMeanSquaredError,
      linRegModel.summary.totalIterations,
      linRegModel)
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {
    validateFormatVersion(formatVersion, 1)
    val linRegModel: LinearRegressionModelMetaData = SaveLoad.extractFromJValue[LinearRegressionModelMetaData](tkMetadata)
    val sparkModel = SparkLinearRegressionModel.read.load(path)

    LinearRegressionModel(linRegModel.valueColumn,
      linRegModel.observationColumns,
      linRegModel.intercept,
      linRegModel.weights,
      linRegModel.explainedVariance,
      linRegModel.meanAbsoluteError,
      linRegModel.meanSquaredError,
      linRegModel.objectiveHistory,
      linRegModel.r2,
      linRegModel.rootMeanSquaredError,
      linRegModel.iterations,
      sparkModel)
  }

  /**
   * Load a PcaModel from the given path
   *
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): LinearRegressionModel = {
    tc.load(path).asInstanceOf[LinearRegressionModel]
  }
}

/**
 *
 * @param valueColumn Frame's column storing the value of the observation
 * @param observationColumnsTrain Frame's column(s) storing the observations
 * @param intercept The intercept of the trained model
 * @param weights Weights of the trained model
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param objectiveHistory Objective function(scaled loss + regularization) at each iteration
 * @param r2 The coefficient of determination of the trained model
 * @param rootMeanSquaredError The square root of the mean squared error
 * @param iterations The number of training iterations until termination
 * @param sparkModel spark ml linear regression model
 */
case class LinearRegressionModel(valueColumn: String,
                                 observationColumnsTrain: Seq[String],
                                 intercept: Double,
                                 weights: Seq[Double],
                                 explainedVariance: Double,
                                 meanAbsoluteError: Double,
                                 meanSquaredError: Double,
                                 objectiveHistory: Seq[Double],
                                 r2: Double,
                                 rootMeanSquaredError: Double,
                                 iterations: Int,
                                 sparkModel: SparkLinearRegressionModel) extends Serializable {
  val predictionColumn = "predicted_value"
  val featuresName = "features"

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame                  Frame to test the linear regression model on
   * @param valueColumn            Column name containing the value of each observation
   * @param observationColumnsTest List of column(s) containing the observations
   * @return linear regression metrics
   *         The data returned is composed of the following:
   *         'explainedVariance' : double
   *         The explained variance regression score
   *         'meanAbsoluteError' : double
   *         The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
   *         'meanSquaredError': double
   *         The risk function corresponding to the expected value of the squared error loss or quadratic loss
   *         'r2' : double
   *         The coefficient of determination
   *         'rootMeanSquaredError' : double
   *         The square root of the mean squared error
   */
  def test(frame: Frame, valueColumn: String, observationColumnsTest: Option[List[String]]) = {
    if (observationColumnsTest.isDefined) {
      require(observationColumnsTrain.length == observationColumnsTest.get.length, "Number of columns for train and test should be same")
    }

    val testFrame = new FrameRdd(frame.schema, frame.rdd).toDataFrame
    val observationColumns = observationColumnsTest.getOrElse(observationColumnsTrain)

    val trainVectors = new VectorAssembler().setInputCols(observationColumns.toArray).setOutputCol(featuresName)

    val dataFrame = trainVectors.transform(testFrame)

    sparkModel.setFeaturesCol(featuresName)
    sparkModel.setPredictionCol(predictionColumn)

    val fullPrediction = sparkModel.transform(dataFrame)
    val predictionLabelRdd = fullPrediction.select(predictionColumn, valueColumn).map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new RegressionMetrics(predictionLabelRdd)

    LinearRegressionTestMetrics(metrics.explainedVariance, metrics.meanAbsoluteError, metrics.meanSquaredError, metrics.r2, metrics.rootMeanSquaredError)
  }

  /**
   * Predict values for a frame using a trained Linear Regression model
   *
   * @param frame The frame to predict on
   * @param observationColumns List of column(s) containing the observations
   * @return returns predicted frame
   */
  def predict(frame: Frame, observationColumns: Option[List[String]]): Frame = {

    require(frame != null, "require frame to predict")

    val predictFrame = new FrameRdd(frame.schema, frame.rdd).toDataFrame
    val trainVectors = new VectorAssembler().setInputCols(observationColumns.getOrElse(observationColumnsTrain.toList).toArray).setOutputCol(featuresName)

    val dataFrame = trainVectors.transform(predictFrame)

    val fullPrediction = sparkModel.transform(dataFrame)

    new Frame(fullPrediction.drop(col(featuresName)))
  }

  /**
   * Saves this model to a file
   *
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.write.save(path)
    val formatVersion: Int = 1
    val tkMetadata = LinearRegressionModelMetaData(valueColumn,
      observationColumnsTrain,
      intercept,
      weights.toArray,
      explainedVariance,
      meanAbsoluteError,
      meanSquaredError,
      objectiveHistory.toArray,
      r2,
      rootMeanSquaredError,
      iterations)

    TkSaveLoad.saveTk(sc, path, LinearRegressionModel.formatId, formatVersion, tkMetadata)
  }
}

/**
 * @param valueColumn Frame's column storing the value of the observation
 * @param observationColumns Frame's column(s) storing the observations
 * @param intercept The intercept of the trained model
 * @param weights Weights of the trained model
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param objectiveHistory Objective function(scaled loss + regularization) at each iteration
 * @param r2 The coefficient of determination of the trained model
 * @param rootMeanSquaredError The square root of the mean squared error
 * @param iterations The number of training iterations until termination
 */
case class LinearRegressionModelMetaData(valueColumn: String,
                                         observationColumns: Seq[String],
                                         intercept: Double,
                                         weights: Array[Double],
                                         explainedVariance: Double,
                                         meanAbsoluteError: Double,
                                         meanSquaredError: Double,
                                         objectiveHistory: Array[Double],
                                         r2: Double,
                                         rootMeanSquaredError: Double,
                                         iterations: Int) extends Serializable

/**
 * Return value from Linear Regression test
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param r2 The coefficient of determination
 * @param rootMeanSquaredError The square root of the mean squared error
 */
case class LinearRegressionTestMetrics(explainedVariance: Double,
                                       meanAbsoluteError: Double,
                                       meanSquaredError: Double,
                                       r2: Double,
                                       rootMeanSquaredError: Double)
