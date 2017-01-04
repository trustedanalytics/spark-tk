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
import org.apache.spark.ml.regression.org.trustedanalytics.sparktk.{ LinearRegressionData, TkLinearRegressionModel }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.models.regression.RegressionUtils._
import org.trustedanalytics.sparktk.models.regression.RegressionColumnNames._
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.spark.ml.regression.{ LinearRegressionModel => SparkLinearRegressionModel }
import org.apache.commons.lang.StringUtils
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, ScoringModelUtils }
import scala.language.implicitConversions
import org.json4s.JsonAST.JValue
import org.apache.spark.mllib.linalg.Vectors
import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils

object LinearRegressionModel extends TkSaveableObject {

  /**
   * Run Spark Ml's LinearRegression on the training frame and create a Model for it.
   *
   * @param frame A frame to train the model on
   * @param observationColumns List of column(s) containing the observations.
   * @param labelColumn Column name containing the label for each observation.
   * @param elasticNetParameter Parameter for the ElasticNet mixing. Default is 0.0
   * @param fitIntercept Parameter for whether to fit an intercept term. Default is true
   * @param maxIterations Parameter for maximum number of iterations. Default is 100
   * @param regParam Parameter for regularization. Default is 0.0
   * @param standardization Parameter for whether to standardize the training features before fitting the model. Default is true
   * @param convergenceTolerance Parameter for the convergence tolerance for iterative algorithms. Default is 1E-6
   * @return returns LinearRegressionModel
   */
  def train(frame: Frame,
            observationColumns: Seq[String],
            labelColumn: String,
            elasticNetParameter: Double = 0.0,
            fitIntercept: Boolean = true,
            maxIterations: Int = 100,
            regParam: Double = 0.0,
            standardization: Boolean = true,
            convergenceTolerance: Double = 1E-6): LinearRegressionModel = {

    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(labelColumn), "labelColumn must not be null nor empty")
    require(maxIterations > 0, "numIterations must be a positive value")
    require(regParam >= 0, "regParam should be greater than or equal to 0")

    // Use DataFrames to run the linear regression
    val trainFrame: DataFrame = new FrameRdd(frame.schema, frame.rdd).toDataFrame
    val trainVectors = new VectorAssembler().setInputCols(observationColumns.toArray).setOutputCol(featuresColName)

    val trainDataFrame: DataFrame = trainVectors.transform(trainFrame)

    val linReg = new LinearRegression()
    linReg.setElasticNetParam(elasticNetParameter)
      .setFitIntercept(fitIntercept)
      .setMaxIter(maxIterations)
      .setRegParam(regParam)
      .setStandardization(standardization)
      .setTol(convergenceTolerance)
      .setLabelCol(labelColumn)
      .setFeaturesCol(featuresColName)

    val linRegModel = linReg.fit(trainDataFrame)

    linRegModel.setPredictionCol(predictionColName)

    LinearRegressionModel(observationColumns,
      labelColumn,
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

    LinearRegressionModel(linRegModel.observationColumns,
      linRegModel.labelColumn,
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
 * Linear regression model
 * @param observationColumns Frame's column(s) storing the observations
 * @param labelColumn Frame's column storing the label of the observation
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
case class LinearRegressionModel(observationColumns: Seq[String],
                                 labelColumn: String,
                                 intercept: Double,
                                 weights: Seq[Double],
                                 explainedVariance: Double,
                                 meanAbsoluteError: Double,
                                 meanSquaredError: Double,
                                 objectiveHistory: Seq[Double],
                                 r2: Double,
                                 rootMeanSquaredError: Double,
                                 iterations: Int,
                                 sparkModel: SparkLinearRegressionModel) extends Serializable with Model {

  // TkLinearRegressionModel, used to accessing protected methods in the Spark LinearRegressionModel
  lazy val tkLinearRegModel = new TkLinearRegressionModel(LinearRegressionData(sparkModel, observationColumns, labelColumn))

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame Frame to test the linear regression model on
   * @param observationColumns List of column(s) containing the observations
   * @param labelColumn Column name containing the label of each observation
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
  def test(frame: Frame, observationColumns: Option[List[String]] = None, labelColumn: Option[String] = None) = {
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and test should be same")
    }

    val testFrame: DataFrame = new FrameRdd(frame.schema, frame.rdd).toDataFrame
    val observations = observationColumns.getOrElse(this.observationColumns).toArray
    val label = labelColumn.getOrElse(this.labelColumn)
    val trainVectors = new VectorAssembler().setInputCols(observations).setOutputCol(featuresColName)

    val testDataFrame: DataFrame = trainVectors.transform(testFrame)

    sparkModel.setFeaturesCol(featuresColName)
    sparkModel.setPredictionCol(predictionColName)

    val predictFrame: DataFrame = sparkModel.transform(testDataFrame)
    getRegressionMetrics(predictFrame, predictionColName, label)
  }

  /**
   * Predict values for a frame using a trained Linear Regression model
   *
   * @param frame The frame to predict on
   * @param observationColumns List of column(s) containing the observations
   * @return returns predicted frame
   */
  def predict(frame: Frame, observationColumns: Option[List[String]] = None): Frame = {

    require(frame != null, "require frame to predict")

    val predictFrame: DataFrame = new FrameRdd(frame.schema, frame.rdd).toDataFrame
    val observations = observationColumns.getOrElse(this.observationColumns).toArray
    val trainVectors = new VectorAssembler().setInputCols(observations).setOutputCol(featuresColName)

    val predictDataFrame: DataFrame = trainVectors.transform(predictFrame)

    val fullPrediction: DataFrame = sparkModel.transform(predictDataFrame)

    new Frame(fullPrediction.drop(col(featuresColName)))
  }

  /**
   * Saves this model to a file
   *
   * @param sc active SparkContext
   * @param path save to path
   * @param overwrite Boolean indicating if the directory will be overwritten, if it already exists.
   */
  def save(sc: SparkContext, path: String, overwrite: Boolean = false): Unit = {

    if (overwrite)
      sparkModel.write.overwrite().save(path)
    else
      sparkModel.write.save(path)
    val formatVersion: Int = 1
    val tkMetadata = LinearRegressionModelMetaData(observationColumns,
      labelColumn,
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

  override def score(data: Array[Any]): Array[Any] = {
    require(data != null && data.length > 0, "scoring data must not be null nor empty")
    val x: Array[Double] = new Array[Double](data.length)
    data.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = ScoringModelUtils.asDouble(value)
    }

    // Call to tkLinearRegModel, since predict() in the spark LinearRegressionModel is protected
    data :+ tkLinearRegModel.vectorPredict(Vectors.dense(x))
  }

  override def modelMetadata(): ModelMetaData = {
    new ModelMetaData("Linear Regression Model", classOf[LinearRegressionModel].getName, classOf[SparkTkModelAdapter].getName, Map())
  }

  override def input(): Array[Field] = {
    val obsCols = observationColumns
    var input = Array[Field]()
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("Prediction", "Double")
  }

  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      // The spark linear regression model save will fail, if we don't specify the "overwrite", since the temp
      // directory has already been created.
      save(sc, tmpDir.toString, overwrite = true)
      ScoringModelUtils.saveToMar(marSavePath, classOf[LinearRegressionModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }
}

/**
 * Metadata for linear regression model
 * @param observationColumns Frame's column(s) storing the observations
 * @param labelColumn Frame's column storing the label of the observation
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
case class LinearRegressionModelMetaData(observationColumns: Seq[String],
                                         labelColumn: String,
                                         intercept: Double,
                                         weights: Array[Double],
                                         explainedVariance: Double,
                                         meanAbsoluteError: Double,
                                         meanSquaredError: Double,
                                         objectiveHistory: Array[Double],
                                         r2: Double,
                                         rootMeanSquaredError: Double,
                                         iterations: Int) extends Serializable

