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
package org.trustedanalytics.sparktk.models.classification.logistic_regression

import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.org.trustedanalytics.sparktk.LogisticRegressionModelWithFrequency
import org.apache.spark.sql.Row
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricsFunctions, ClassificationMetricValue }
import org.trustedanalytics.sparktk.frame.internal.rdd.{ ScoreAndLabel, RowWrapperFunctions, FrameRdd }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import scala.language.implicitConversions
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }
import org.apache.spark.mllib.linalg.DenseVector
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, ScoringModelUtils }
import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils

object LogisticRegressionModel extends TkSaveableObject {

  /**
   * Build logistic regression model.
   *
   * Create a logistic regression model and train it using the obseravtion columns and label column of a given frame
   *
   * @param frame                A frame to train the model on.
   * @param observationColumns   Column(s) containing the observations.
   * @param labelColumn          Column name containing the label for each observation.
   * @param frequencyColumn      Optional column containing the frequency of observations.
   * @param numClasses           Number of classes
   *                             numClasses should not exceed the number of distinct values in labelColumn
   * @param optimizer            Set type of optimizer.
   *                             LBFGS - Limited-memory BFGS.
   *                             LBFGS supports multinomial logistic regression.
   *                             SGD - Stochastic Gradient Descent.
   *                             SGD only supports binary logistic regression.
   * @param computeCovariance    Compute covariance matrix for the model.
   * @param intercept            Add intercept column to training data.
   * @param featureScaling       Perform feature scaling before training model.
   * @param threshold            Threshold for separating positive predictions from negative predictions.
   * @param regType              Set type of regularization
   *                             L1 - L1 regularization with sum of absolute values of coefficients
   *                             L2 - L2 regularization with sum of squares of coefficients
   * @param regParam             Regularization parameter
   * @param numIterations        Maximum number of iterations
   * @param convergenceTolerance Convergence tolerance of iterations for L-BFGS. Smaller value will lead to higher accuracy with the cost of more iterations.
   * @param numCorrections       Number of corrections used in LBFGS update.
   *                             Default is 10.
   *                             Values of less than 3 are not recommended;
   *                             large values will result in excessive computing time.
   * @param miniBatchFraction    Fraction of data to be used for each SGD iteration
   * @param stepSize             Initial step size for SGD. In subsequent steps, the step size decreases by stepSize/sqrt(t)
   * @return A LogisticRegressionModel with a summary of the trained model.
   *         The data returned is composed of multiple components\:
   *         **int** : *numFeatures*
   *         Number of features in the training data
   *         **int** : *numClasses*
   *         Number of classes in the training data
   *         **table** : *summaryTable*
   *         A summary table composed of:
   *         **Frame** : *CovarianceMatrix (optional)*
   *         Covariance matrix of the trained model.
   *         The covariance matrix is the inverse of the Hessian matrix for the trained model.
   *         The Hessian matrix is the second-order partial derivatives of the model's log-likelihood function.
   */
  def train(frame: Frame,
            observationColumns: Seq[String],
            labelColumn: String,
            frequencyColumn: Option[String] = None,
            numClasses: Int = 2,
            optimizer: String = "LBFGS",
            computeCovariance: Boolean = true,
            intercept: Boolean = true,
            featureScaling: Boolean = false,
            threshold: Double = 0.5,
            regType: String = "L2",
            regParam: Double = 0,
            numIterations: Int = 100,
            convergenceTolerance: Double = 0.0001,
            numCorrections: Int = 10,
            miniBatchFraction: Double = 1d,
            stepSize: Double = 1d) = {

    require(frame != null, "frame is required")
    require(optimizer == "LBFGS" || optimizer == "SGD", "optimizer name must be 'LBFGS' or 'SGD'")
    require(numClasses > 1, "number of classes must be greater than 1")
    if (optimizer == "SGD") require(numClasses == 2, "multinomial logistic regression not supported for SGD")
    require(observationColumns != null && observationColumns.nonEmpty, "observation columns must not be null nor empty")
    require(labelColumn != null && !labelColumn.isEmpty, "label column must not be null nor empty")
    require(numIterations > 0, "number of iterations must be a positive value")
    require(regType == "L1" || regType == "L2", "regularization type must be 'L1' or 'L2'")
    require(convergenceTolerance > 0, "convergence tolerance for LBFGS must be a positive value")
    require(numCorrections > 0, "number of corrections for LBFGS must be a positive value")
    require(miniBatchFraction > 0, "mini-batch fraction for SGD must be a positive value")
    require(stepSize > 0, "step size for SGD must be a positive value")

    frame.schema.validateColumnsExist(observationColumns :+ labelColumn)

    val arguments = LogisticRegressionTrainArgs(frame,
      observationColumns.toList,
      labelColumn,
      frequencyColumn,
      numClasses,
      optimizer,
      computeCovariance,
      intercept,
      featureScaling,
      threshold,
      regType,
      regParam,
      numIterations,
      convergenceTolerance,
      numCorrections,
      miniBatchFraction,
      stepSize)

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)

    //create RDD from the frame
    val labeledTrainRdd = frameRdd.toLabeledPointRDDWithFrequency(labelColumn, observationColumns.toList, frequencyColumn)

    //Running MLLib
    val mlModel = LogisticRegressionModelWrapperFactory.createModel(arguments)
    val sparkLogRegModel = mlModel.getModel.run(labeledTrainRdd)

    val trainingSummary = buildSummaryTable(frame.rdd.sparkContext, sparkLogRegModel, observationColumns, intercept, mlModel.getHessianMatrix)

    LogisticRegressionModel(observationColumns.toList,
      labelColumn,
      frequencyColumn,
      numClasses,
      optimizer,
      computeCovariance,
      intercept,
      featureScaling,
      threshold,
      regType,
      regParam,
      numIterations,
      convergenceTolerance,
      numCorrections,
      miniBatchFraction,
      stepSize,
      trainingSummary,
      mlModel.getHessianMatrix,
      sparkLogRegModel)
  }

  /**
   *
   * @param sc            active spark context
   * @param path          the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata    the data to save (should be a case class), must be serializable to JSON using json4s
   */
  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: LogisticRegressionModelMetaData = SaveLoad.extractFromJValue[LogisticRegressionModelMetaData](tkMetadata)
    val sparkLogRegModel: LogisticRegressionModelWithFrequency = LogisticRegressionModelWithFrequency.load(sc, path)

    val hessianMatrixNew: Option[DenseMatrix[Double]] = m.hessianMatrixData match {
      case null => None
      case other => Some(new DenseMatrix(m.hessianMatrixRows, m.hessianMatrixCols, m.hessianMatrixData))
    }

    val finalSummaryTable = buildSummaryTable(sc, sparkLogRegModel, m.observationColumns, m.intercept, hessianMatrixNew)

    LogisticRegressionModel(m.observationColumns,
      m.labelColumn,
      m.frequencyColumn,
      m.numClasses,
      m.optimizer,
      m.computeCovariance,
      m.intercept,
      m.featureScaling,
      m.threshold,
      m.regType,
      m.regParam,
      m.numIterations,
      m.convergenceTolerance,
      m.numCorrections,
      m.miniBatchFraction,
      m.stepSize,
      finalSummaryTable,
      hessianMatrixNew,
      sparkLogRegModel)
  }

  /**
   * Load a PcaModel from the given path
   *
   * @param tc   TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): LogisticRegressionModel = {
    tc.load(path).asInstanceOf[LogisticRegressionModel]
  }

  //Helper to build logistic regressioin summary table
  def buildSummaryTable(sc: SparkContext,
                        sparkLogRegModel: LogisticRegressionModelWithFrequency,
                        observationColumns: Seq[String],
                        intercept: Boolean,
                        hessianMatrix: Option[DenseMatrix[Double]]): LogisticRegressionSummaryTable = {

    //Create summary table and covariance frame
    val summaryTable = SummaryTableBuilder(sparkLogRegModel,
      observationColumns.toList,
      intercept,
      hessianMatrix)

    val covarianceFrame = summaryTable.approxCovarianceMatrix match {
      case Some(matrix) =>
        val coFrameRdd = matrix.toFrameRdd(sc, summaryTable.coefficientNames)
        val coFrame = new Frame(coFrameRdd.rdd, coFrameRdd.schema)
        Some(coFrame)
      case _ => None
    }
    summaryTable.build(covarianceFrame)
  }
}

/**
 * Logistic Regression Model
 *
 * @param observationColumns   Column(s) containing the observations.
 * @param labelColumn          Column name containing the label for each observation.
 * @param frequencyColumn      Optional column containing the frequency of observations.
 * @param numClasses           Number of classes
 * @param optimizer            Set type of optimizer.
 *                             LBFGS - Limited-memory BFGS.
 *                             LBFGS supports multinomial logistic regression.
 *                             SGD - Stochastic Gradient Descent.
 *                             SGD only supports binary logistic regression.
 * @param computeCovariance    Compute covariance matrix for the model.
 * @param intercept            Add intercept column to training data.
 * @param featureScaling       Perform feature scaling before training model.
 * @param threshold            Threshold for separating positive predictions from negative predictions.
 * @param regType              Set type of regularization
 *                             L1 - L1 regularization with sum of absolute values of coefficients
 *                             L2 - L2 regularization with sum of squares of coefficients
 * @param regParam             Regularization parameter
 * @param numIterations        Maximum number of iterations
 * @param convergenceTolerance Convergence tolerance of iterations for L-BFGS. Smaller value will lead to higher accuracy with the cost of more iterations.
 * @param numCorrections       Number of corrections used in LBFGS update.
 *                             Default is 10.
 *                             Values of less than 3 are not recommended;
 *                             large values will result in excessive computing time.
 * @param miniBatchFraction    Fraction of data to be used for each SGD iteration
 * @param stepSize             Initial step size for SGD. In subsequent steps, the step size decreases by stepSize/sqrt(t)
 * @param trainingSummary      logistic regression training summary table
 * @param hessianMatrix        hessianMatrix
 * @param sparkModel           Spark LogisticRegressionModel
 */
case class LogisticRegressionModel private[logistic_regression] (observationColumns: List[String],
                                                                 labelColumn: String,
                                                                 frequencyColumn: Option[String],
                                                                 numClasses: Int,
                                                                 optimizer: String,
                                                                 computeCovariance: Boolean,
                                                                 intercept: Boolean,
                                                                 featureScaling: Boolean,
                                                                 threshold: Double,
                                                                 regType: String,
                                                                 regParam: Double,
                                                                 numIterations: Int,
                                                                 convergenceTolerance: Double,
                                                                 numCorrections: Int,
                                                                 miniBatchFraction: Double,
                                                                 stepSize: Double,
                                                                 trainingSummary: LogisticRegressionSummaryTable,
                                                                 hessianMatrix: Option[DenseMatrix[Double]],
                                                                 sparkModel: LogisticRegressionModelWithFrequency) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Predict labels for data points using trained logistic regression model.
   *
   * Predict the labels for a test frame using trained logistic regression model, and create a new frame revision with
   * existing columns and a new predicted label's column.
   *
   * @param frame                     A frame whose labels are to be predicted. By default, predict is run on the same columns over which the model is trained.
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted. Default is the labels the model was trained on.
   * @return Frame containing the original frame's columns and a column with the predicted label.
   */
  def predict(frame: Frame, observationColumns: Option[List[String]] = None): Frame = {
    require(frame != null, "frame is required")

    //Running MLLib
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length,
        "Number of columns for train and predict should be same")
    }
    val observations = observationColumns.getOrElse(this.observationColumns)

    //predicting a label for the observation columns
    val predictColumn = Column(frame.schema.getNewColumnName("predicted_label"), DataTypes.int32)

    val predictMapper: RowWrapper => Row = row => {
      val point = row.valuesAsDenseVector(observations)
      val prediction = sparkModel.predict(point).toInt
      Row.apply(prediction)
    }

    val predictSchema = frame.schema.addColumn(predictColumn)
    val wrapper = new RowWrapper(predictSchema)
    val predictRdd = frame.rdd.map(row => Row.merge(row, predictMapper(wrapper(row))))

    new Frame(predictRdd, predictSchema)
  }

  /**
   * Saves this model to a file
   *
   * @param sc   active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path)
    val formatVersion: Int = 1
    val (hessMatrixRows, hessMatrixCols, hessMatrixDataArray) = hessianMatrix match {
      case Some(matrix) => (matrix.rows, matrix.cols, matrix.data)
      case None => (0, 0, null)
    }
    val tkMetaData = LogisticRegressionModelMetaData(observationColumns.toList,
      labelColumn,
      frequencyColumn,
      numClasses,
      optimizer,
      computeCovariance,
      intercept,
      featureScaling,
      threshold,
      regType,
      regParam,
      numIterations,
      convergenceTolerance,
      numCorrections,
      miniBatchFraction,
      stepSize,
      hessMatrixRows,
      hessMatrixCols,
      hessMatrixDataArray)

    TkSaveLoad.saveTk(sc, path, LogisticRegressionModel.formatId, formatVersion, tkMetaData)
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame                  Frame whose labels are to be predicted.
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted and tested. Default is to test over the columns the SVM model was trained on.
   * @param labelColumn            Column containing the actual label for each observation.
   * @return A dictionary with binary classification metrics.
   *         The data returned is composed of the following keys\:
   *         'accuracy' : double
   *         The proportion of predictions that are correctly identified
   *         'confusion_matrix' : dictionary
   *         A table used to describe the performance of a classification model
   *         'f_measure' : double
   *         The harmonic mean of precision and recall
   *         'precision' : double
   *         The proportion of predicted positive instances that are correctly identified
   *         'recall' : double
   *         The proportion of positive instances that are correctly identified.
   * //
   */
  def test(frame: Frame, observationColumns: Option[List[String]] = None, labelColumn: Option[String] = None): ClassificationMetricValue = {
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and test should be same")
    }
    val observations = observationColumns.getOrElse(this.observationColumns)
    val label = labelColumn.getOrElse(this.labelColumn)

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    //predicting and testing
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(observations, label)
      val score = sparkModel.predict(labeledPoint.features)
      ScoreAndLabel(score, labeledPoint.label)
    })

    //Run classification metrics
    sparkModel.numClasses match {
      case 2 => {
        val posLabel: Double = 1.0d
        ClassificationMetricsFunctions.binaryClassificationMetrics(scoreAndLabelRdd, posLabel)
      }
      case _ => ClassificationMetricsFunctions.multiclassClassificationMetrics(scoreAndLabelRdd)
    }
  }

  override def score(row: Array[Any]): Array[Any] = {
    require(row != null && row.length > 0, "scoring input row must not be null nor empty")
    val doubleArray = row.map(i => ScoringModelUtils.asDouble(i))
    val predictedLabel = sparkModel.predict(new DenseVector(doubleArray)).toInt
    row :+ predictedLabel
  }

  override def modelMetadata(): ModelMetaData = {
    new ModelMetaData("Logistic Regression", classOf[LogisticRegressionModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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
    output :+ Field("PredictedLabel", "Int")
  }

  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[LogisticRegressionModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }
}

/**
 * Logistic Regression Meta data
 *
 * @param observationColumns   Column(s) containing the observations.
 * @param labelColumn          Column name containing the label for each observation.
 * @param frequencyColumn      Optional column containing the frequency of observations.
 * @param numClasses           Number of classes
 * @param optimizer            Set type of optimizer.
 *                             LBFGS - Limited-memory BFGS.
 *                             LBFGS supports multinomial logistic regression.
 *                             SGD - Stochastic Gradient Descent.
 *                             SGD only supports binary logistic regression.
 * @param computeCovariance    Compute covariance matrix for the model.
 * @param intercept            Add intercept column to training data.
 * @param featureScaling       Perform feature scaling before training model.
 * @param threshold            Threshold for separating positive predictions from negative predictions.
 * @param regType              Set type of regularization
 *                             L1 - L1 regularization with sum of absolute values of coefficients
 *                             L2 - L2 regularization with sum of squares of coefficients
 * @param regParam             Regularization parameter
 * @param numIterations        Maximum number of iterations
 * @param convergenceTolerance Convergence tolerance of iterations for L-BFGS. Smaller value will lead to higher accuracy with the cost of more iterations.
 * @param numCorrections       Number of corrections used in LBFGS update.
 *                             Default is 10.
 *                             Values of less than 3 are not recommended;
 *                             large values will result in excessive computing time.
 * @param miniBatchFraction    Fraction of data to be used for each SGD iteration
 * @param stepSize             Initial step size for SGD. In subsequent steps, the step size decreases by stepSize/sqrt(t)
 * @param hessianMatrixRows    hessian matrix rows count
 * @param hessianMatrixCols    hessian matrix cols count
 * @param hessianMatrixData    hessian matrix data array
 */
case class LogisticRegressionModelMetaData(observationColumns: List[String],
                                           labelColumn: String,
                                           frequencyColumn: Option[String],
                                           numClasses: Int,
                                           optimizer: String,
                                           computeCovariance: Boolean,
                                           intercept: Boolean,
                                           featureScaling: Boolean,
                                           threshold: Double,
                                           regType: String,
                                           regParam: Double,
                                           numIterations: Int,
                                           convergenceTolerance: Double,
                                           numCorrections: Int,
                                           miniBatchFraction: Double,
                                           stepSize: Double,
                                           hessianMatrixRows: Int,
                                           hessianMatrixCols: Int,
                                           hessianMatrixData: Array[Double]) extends Serializable

/**
 * Input arguments for logistic regression train plugin
 */
case class LogisticRegressionTrainArgs(frame: Frame,
                                       observationColumns: List[String],
                                       labelColumn: String,
                                       frequencyColumn: Option[String],
                                       numClasses: Int,
                                       optimizer: String,
                                       computeCovariance: Boolean,
                                       intercept: Boolean,
                                       featureScaling: Boolean,
                                       threshold: Double,
                                       regType: String,
                                       regParam: Double,
                                       numIterations: Int,
                                       convergenceTolerance: Double,
                                       numCorrections: Int,
                                       miniBatchFraction: Double,
                                       stepSize: Double)