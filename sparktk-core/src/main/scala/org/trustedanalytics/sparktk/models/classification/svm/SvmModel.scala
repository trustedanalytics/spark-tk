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
package org.trustedanalytics.sparktk.models.classification.svm

import java.nio.file.{ Files, Path }

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ SVMModel => SparkSvmModel, SVMWithSGD }
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.io.{ FileUtils }
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, ScoringModelUtils }
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricsFunctions, ClassificationMetricValue }
import org.trustedanalytics.sparktk.frame.internal.rdd.{ RowWrapperFunctions, ScoreAndLabel, FrameRdd }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.commons.lang3.StringUtils

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object SvmModel extends TkSaveableObject {

  /**
   * Train a SVM with SGD Model using the observation column and label column of a frame.
   *
   * @param frame The frame containing the data to train on
   * @param labelColumn Column containing the label for each observation
   * @param observationColumns Column(s) containing the observations
   * @param intercept Flag indicating if the algorithm adds an intercept. Default is true
   * @param numIterations Number of iterations for SGD. Default is 100
   * @param stepSize Initial step size for SGD optimizer for the first step. Default is 1.0
   * @param regType Regularization "L1" or "L2". Default is "L2"
   * @param regParam Regularization parameter. Default is 0.01
   * @param miniBatchFraction Set fraction of data to be used for each SGD iteration. Default is 1.0; corresponding to deterministic/classical gradient descent
   */

  def train(frame: Frame,
            labelColumn: String,
            observationColumns: Seq[String],
            intercept: Boolean = true,
            numIterations: Int = 100,
            stepSize: Double = 1d,
            regType: Option[String] = None,
            regParam: Double = 0.01,
            miniBatchFraction: Double = 1.0): SvmModel = {
    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(labelColumn), "labelColumn must not be null nor empty")
    require(!frame.rdd.isEmpty(), "Frame is empty. Please train on a non-empty Frame.")
    require(numIterations > 0, "number of iterations must be a positive value")
    require(regType.isEmpty || List("L1", "L2").contains(regType), "regularization type can be either empty or" +
      "one of the 2 values: L1 or L2")

    frame.schema.validateColumnsExist(observationColumns)

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = FrameRdd.toLabeledPointRDD(new FrameRdd(frame.schema, frame.rdd), labelColumn, observationColumns)

    val svm = new SVMWithSGD()
    svm.optimizer.setNumIterations(numIterations)
    svm.optimizer.setStepSize(stepSize)
    svm.optimizer.setRegParam(regParam)

    if (regType.isDefined) {
      svm.optimizer.setUpdater(regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }
    svm.optimizer.setMiniBatchFraction(miniBatchFraction)
    svm.setIntercept(intercept)

    //Running MLLib
    val svmModel = svm.run(labeledTrainRdd)

    SvmModel(svmModel, labelColumn,
      observationColumns,
      intercept,
      numIterations,
      stepSize,
      regType,
      regParam,
      miniBatchFraction)
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: SvmModelTkMetaData = SaveLoad.extractFromJValue[SvmModelTkMetaData](tkMetadata)
    val sparkModel = SparkSvmModel.load(sc, path)

    SvmModel(sparkModel,
      m.labelColumn,
      m.observationColumns,
      m.intercept,
      m.numIterations,
      m.stepSize,
      m.regType,
      m.regParam,
      m.miniBatchFraction)
  }

  /**
   * Load a SvmModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): SvmModel = {
    tc.load(path).asInstanceOf[SvmModel]
  }
}

/**
 * SvmModel
 * @param sparkModel Trained MLLib's Naive Bayes model
 * @param labelColumn Label column for trained model
 * @param observationColumns Handle to the observation columns of the data frame
 * @param intercept Flag indicating if the algorithm adds an intercept
 * @param numIterations Number of iterations for SGD
 * @param stepSize Initial step size for SGD optimizer for the first step
 * @param regType Regularization "L1" or "L2"
 * @param regParam Regularization parameter
 * @param miniBatchFraction Set fraction of data to be used for each SGD iteration. corresponding to deterministic/classical gradient descent
 */
case class SvmModel private[svm] (sparkModel: SparkSvmModel,
                                  labelColumn: String,
                                  observationColumns: Seq[String],
                                  intercept: Boolean,
                                  numIterations: Int,
                                  stepSize: Double,
                                  regType: Option[String],
                                  regParam: Double,
                                  miniBatchFraction: Double) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Predicts the labels for the observation columns in the input frame
   * @param frame - frame to add predictions to
   * @param columns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the SvmModel was trained on
   * @return New frame containing the original frame's columns and a column with the predicted label
   */
  def predict(frame: Frame, columns: Option[List[String]] = None): Frame = {
    require(frame != null, "frame is required")
    if (columns.isDefined) {
      require(columns.get.length == observationColumns.length, "Number of columns for train and predict should be same")
    }

    val svmColumns = columns.getOrElse(observationColumns)
    val predictMapper: RowWrapper => Row = row => {
      val point = row.toDenseVector(svmColumns)
      val prediction = sparkModel.predict(point).toInt
      Row.apply(prediction)
    }
    val predictSchema = frame.schema.addColumn(Column("predicted_label", DataTypes.int32))
    val wrapper = new RowWrapper(predictSchema)
    val predictRdd = frame.rdd.map(row => Row.merge(row, predictMapper(wrapper(row))))

    new Frame(predictRdd, predictSchema)
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame Frame to test the Svm model
   * @param columns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the SvmModel
   * @return ClassificationMetricValue describing the test metrics
   */
  def test(frame: Frame, columns: Option[List[String]]): ClassificationMetricValue = {

    if (columns.isDefined) {
      require(columns.get.length == observationColumns.length, "Number of columns for train and test should be same")
    }
    val svmColumns = columns.getOrElse(observationColumns)

    //predicting and testing
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(svmColumns, labelColumn)
      val score = sparkModel.predict(labeledPoint.features)
      ScoreAndLabel(score, labeledPoint.label)
    })

    //Run Binary classification metrics
    val posLabel: Double = 1d
    ClassificationMetricsFunctions.binaryClassificationMetrics(scoreAndLabelRdd, posLabel)
  }

  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path)
    val formatVersion: Int = 1
    val tkMetadata = SvmModelTkMetaData(labelColumn,
      observationColumns,
      intercept,
      numIterations,
      stepSize,
      regType,
      regParam,
      miniBatchFraction)
    TkSaveLoad.saveTk(sc, path, SvmModel.formatId, formatVersion, tkMetadata)
  }

  /**
   * gets the prediction on the provided record
   * @param row a record that needs to be predicted on
   * @return the row along with its prediction
   */
  def score(row: Array[Any]): Array[Any] = {
    val x: Array[Double] = row.map(y => ScoringModelUtils.asDouble(y))
    row :+ sparkModel.predict(Vectors.dense(x))
  }

  /**
   * @return fields containing the input names and their datatypes
   */
  def input(): Array[Field] = {
    var input = Array[Field]()
    observationColumns.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  def output(): Array[Field] = {
    val output = input()
    output :+ Field("Prediction", "Double")
  }

  /**
   * @return metadata about the model
   */
  def modelMetadata(): ModelMetaData = {
    //todo provide an API for the user to populate the custom metadata fields
    new ModelMetaData("SVM with SGD Model", classOf[SvmModel].getName, classOf[SparkTkModelAdapter].getName, Map())
  }

  /**
   * @param sc active SparkContext
   * @param marSavePath location where the MAR file needs to be saved
   * @return full path to the location of the MAR file
   */
  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, "file://" + tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[SvmModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }

}

/**
 * TK Metadata that will be stored as part of the model
 * @param labelColumn Label column for trained model
 * @param observationColumns Handle to the observation columns of the data frame
 * @param intercept Flag indicating if the algorithm adds an intercept
 * @param numIterations Number of iterations for SGD
 * @param stepSize Initial step size for SGD optimizer for the first step
 * @param regType Regularization "L1" or "L2"
 * @param regParam Regularization parameter
 * @param miniBatchFraction Set fraction of data to be used for each SGD iteration. corresponding to deterministic/classical gradient descent
 */
case class SvmModelTkMetaData(labelColumn: String,
                              observationColumns: Seq[String],
                              intercept: Boolean,
                              numIterations: Int,
                              stepSize: Double,
                              regType: Option[String],
                              regParam: Double,
                              miniBatchFraction: Double) extends Serializable
