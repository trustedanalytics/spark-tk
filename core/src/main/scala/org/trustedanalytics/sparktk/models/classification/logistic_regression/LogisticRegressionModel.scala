package org.trustedanalytics.sparktk.models.classification.logistic_regression

import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.json4s.JsonAST.JValue
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricsFunctions, ClassificationMetricValue }
import org.trustedanalytics.sparktk.frame.internal.rdd.{ ScoreAndLabel, RowWrapperFunctions, FrameRdd }
import org.trustedanalytics.sparktk.models.FrameRddFunctions
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import scala.language.implicitConversions

object LogisticRegressionModel extends TkSaveableObject {

  /**
   * Build logistic regression model.
   *
   * Creating a Logistic Regression Model using the observation column and label column of the train frame.
   *
   * @param frame                A frame to train the model on.
   * @param labelColumn          Column name containing the label for each observation.
   * @param observationColumns   Column(s) containing the observations.
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
            labelColumn: String,
            observationColumns: Seq[String],
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

    implicit def frameToFrameRddFunctions(frame: FrameRdd): FrameRddFunctions = {
      new FrameRddFunctions(frame)
    }

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

    val arguments = LogisticRegressionTrainArgs(frame,
      labelColumn,
      observationColumns.toList,
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
    val logRegModel = mlModel.getModel.run(labeledTrainRdd)

    val finalSummaryTable = buildSummaryTable(frame.rdd.sparkContext, logRegModel, observationColumns, intercept, mlModel.getHessianMatrix)

    LogisticRegressionModel(finalSummaryTable.numFeatures,
      finalSummaryTable.numClasses,
      finalSummaryTable.coefficients,
      finalSummaryTable.degreesFreedom,
      finalSummaryTable.covarianceMatrix,
      finalSummaryTable.standardErrors,
      finalSummaryTable.waldStatistic,
      finalSummaryTable.pValue,
      observationColumns.toList,
      intercept,
      mlModel.getHessianMatrix,
      logRegModel)
  }

  /**
   *
   * @param sc            active spark context
   * @param path          the source path
   * @param formatVersion the version of the format for the tk metadata that should be recorded.
   * @param tkMetadata    the data to save (should be a case class), must be serializable to JSON using json4s
   */
  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: LogisticRegressionModelMetaData = SaveLoad.extractFromJValue[LogisticRegressionModelMetaData](tkMetadata)
    val sparkLogRegModel: LogisticRegressionModelWithFrequency = LogisticRegressionModelWithFrequency.load(sc, path)

    val hessianMatrixNew: Option[DenseMatrix[Double]] = m.hessianMatrixData match {
      case null => None
      case other => Some(new DenseMatrix(m.hessianMatrixRows, m.hessianMatrixCols, m.hessianMatrixData))
    }

    val finalSummaryTable = buildSummaryTable(sc, sparkLogRegModel, m.observationColumns, m.intercept, hessianMatrixNew)

    LogisticRegressionModel(finalSummaryTable.numFeatures,
      finalSummaryTable.numClasses,
      finalSummaryTable.coefficients,
      finalSummaryTable.degreesFreedom,
      finalSummaryTable.covarianceMatrix,
      finalSummaryTable.standardErrors,
      finalSummaryTable.waldStatistic,
      finalSummaryTable.pValue,
      m.observationColumns,
      m.intercept,
      hessianMatrixNew,
      sparkLogRegModel)
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
 * @param numFeatures        Number of features
 * @param numClasses         Number of classes
 * @param coefficients       Model coefficients
 *                           The dimension of the coefficients' vector is
 *                           (numClasses - 1) * (numFeatures + 1) if `addIntercept == true`, and
 *                           (numClasses - 1) * numFeatures if `addIntercept != true`
 * @param degreesFreedom     Degrees of freedom for model coefficients
 * @param covarianceMatrix   Optional covariance matrix
 * @param standardErrors     Optional standard errors for model coefficients
 *                           The standard error for each variable is the square root of
 *                           the diagonal of the covariance matrix
 * @param waldStatistic      Optional Wald Chi-Squared statistic
 *                           The Wald Chi-Squared statistic is the coefficients
 *                           divided by the standard errors
 * @param pValue             Optional p-values for the model coefficients
 * @param observationColumns Column(s) containing the observations.
 * @param intercept          intercept column to training data.
 * @param sparkModel         Spark LogisticRegressionModel
 */
case class LogisticRegressionModel private[logistic_regression] (numFeatures: Int,
                                                                 numClasses: Int,
                                                                 coefficients: Map[String, Double],
                                                                 degreesFreedom: Map[String, Double],
                                                                 covarianceMatrix: Option[Frame],
                                                                 standardErrors: Option[Map[String, Double]],
                                                                 waldStatistic: Option[Map[String, Double]],
                                                                 pValue: Option[Map[String, Double]],
                                                                 observationColumns: List[String],
                                                                 intercept: Boolean,
                                                                 hessianMatrix: Option[DenseMatrix[Double]],
                                                                 sparkModel: LogisticRegressionModelWithFrequency) extends Serializable {

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
   * @param observationColumnsPredict Column(s) containing the observations whose labels are to be predicted. Default is the labels the model was trained on.
   * @return Frame containing the original frame's columns and a column with the predicted label.
   */
  def predict(frame: Frame, observationColumnsPredict: Option[List[String]]): Frame = {
    require(frame != null, "frame is required")

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    //Running MLLib
    if (observationColumnsPredict.isDefined) {
      require(observationColumns.length == observationColumnsPredict.get.length,
        "Number of columns for train and predict should be same")
    }
    val logRegColumns = observationColumnsPredict.getOrElse(observationColumns)

    //predicting a label for the observation columns
    val predictColumn = Column("predicted_label", DataTypes.int32)
    val predictFrameRdd = frameRdd.addColumn(predictColumn, row => {
      val point = row.valuesAsDenseVector(logRegColumns)
      sparkModel.predict(point).toInt
    })

    new Frame(predictFrameRdd.rdd, predictFrameRdd.schema)
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
    val tkMetaData = LogisticRegressionModelMetaData(observationColumns, intercept, hessMatrixRows, hessMatrixCols, hessMatrixDataArray)
    TkSaveLoad.saveTk(sc, path, LogisticRegressionModel.formatId, formatVersion, tkMetaData)
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame                  Frame whose labels are to be predicted.
   * @param labelColumn            Column containing the actual label for each observation.
   * @param observationColumnsTest Column(s) containing the observations whose labels are to be predicted and tested. Default is to test over the columns the SVM model was trained on.
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
   */
  def test(frame: Frame, labelColumn: String, observationColumnsTest: Option[List[String]]): ClassificationMetricValue = {
    if (observationColumnsTest.isDefined) {
      require(observationColumns.length == observationColumnsTest.get.length, "Number of columns for train and test should be same")
    }
    val logRegColumns = observationColumnsTest.getOrElse(observationColumns)

    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    //predicting and testing
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(logRegColumns, labelColumn)
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

}

/**
 * Logistic Regression Meta data
 *
 * @param observationColumns Column(s) containing the observations.
 * @param intercept          Intercept column to training data.
 *
 */
case class LogisticRegressionModelMetaData(observationColumns: List[String], intercept: Boolean, hessianMatrixRows: Int, hessianMatrixCols: Int, hessianMatrixData: Array[Double]) extends Serializable
/**
 * Input arguments for logistic regression train plugin
 */
case class LogisticRegressionTrainArgs(frame: Frame,
                                       labelColumn: String,
                                       observationColumns: List[String],
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