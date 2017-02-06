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
package org.trustedanalytics.sparktk.models.classification.naive_bayes

import java.nio.file.{ Files, Path }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ NaiveBayesModel => SparkNaiveBayesModel, NaiveBayes }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricsFunctions, ClassificationMetricValue }
import org.trustedanalytics.sparktk.frame.internal.rdd.{ RowWrapperFunctions, ScoreAndLabel, FrameRdd }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.commons.lang3.StringUtils
import org.apache.commons.io.{ FileUtils }
import org.trustedanalytics.sparktk.models.{ SparkTkModelAdapter, ScoringModelUtils }
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object NaiveBayesModel extends TkSaveableObject {

  /**
   * @param frame The frame containing the data to train on
   * @param labelColumn Column containing the label for each observation
   * @param observationColumns Column(s) containing the observations
   * @param lambdaParameter Additive smoothing parameter Default is 1.0
   */
  def train(frame: Frame,
            observationColumns: List[String],
            labelColumn: String,
            lambdaParameter: Double = 1.0): NaiveBayesModel = {
    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(labelColumn), "labelColumn must not be null nor empty")

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = FrameRdd.toLabeledPointRDD(new FrameRdd(frame.schema, frame.rdd), labelColumn, observationColumns)

    //Running MLLib
    val naiveBayes = new NaiveBayes()
    naiveBayes.setLambda(lambdaParameter)

    val naiveBayesModel = naiveBayes.run(labeledTrainRdd)
    NaiveBayesModel(naiveBayesModel, observationColumns, labelColumn, lambdaParameter)
  }

  def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: NaiveBayesModelTkMetaData = SaveLoad.extractFromJValue[NaiveBayesModelTkMetaData](tkMetadata)
    val sparkModel = SparkNaiveBayesModel.load(sc, path)

    NaiveBayesModel(sparkModel, m.observationColumns, m.labelColumn, m.lambdaParameter)
  }

  /**
   * Load a NaiveBayesModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): NaiveBayesModel = {
    tc.load(path).asInstanceOf[NaiveBayesModel]
  }
}

/**
 * NaiveBayesModel
 * @param sparkModel Trained MLLib's Naive Bayes model
 * @param observationColumns Handle to the observation columns of the data frame
 * @param labelColumn Label column for trained model
 * @param lambdaParameter Smoothing parameter used during model training
 */
case class NaiveBayesModel private[naive_bayes] (sparkModel: SparkNaiveBayesModel,
                                                 observationColumns: List[String],
                                                 labelColumn: String,
                                                 lambdaParameter: Double) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Predicts the labels for the observation columns in the input frame
   * @param frame - frame to add predictions to
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the NaiveBayesModel was trained on
   * @return New frame containing the original frame's columns and a column with the predicted label
   */
  def predict(frame: Frame, observationColumns: Option[List[String]] = None): Frame = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and predict should be same")
    }

    val observations = observationColumns.getOrElse(this.observationColumns)
    val predictMapper: RowWrapper => Row = row => {
      val point = row.toDenseVector(observations)
      val prediction = sparkModel.predict(point)
      Row.apply(prediction)
    }

    val predictSchema = frame.schema.addColumn(Column("predicted_class", DataTypes.float64))
    val wrapper = new RowWrapper(predictSchema)
    val predictRdd = frame.rdd.map(row => Row.merge(row, predictMapper(wrapper(row))))
    new Frame(predictRdd, predictSchema)
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame Frame to test the NaiveBayes model
   * @param labelColumn Name of the column containing the true labels
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the NaiveBayesModel
   * @return ClassificationMetricValue describing the test metrics
   */
  def test(frame: Frame, observationColumns: Option[List[String]] = None, labelColumn: Option[String] = None): ClassificationMetricValue = {

    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and test should be same")
    }
    val observations = observationColumns.getOrElse(this.observationColumns)
    val label = labelColumn.getOrElse(this.labelColumn)

    //predicting and testing
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(observations, label)
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
    val tkMetadata = NaiveBayesModelTkMetaData(observationColumns, labelColumn, lambdaParameter)
    TkSaveLoad.saveTk(sc, path, NaiveBayesModel.formatId, formatVersion, tkMetadata)
  }

  /**
   * gets the prediction on the provided record
   * @param row a record that needs to be predicted on
   * @return the row along with its prediction
   */
  def score(row: Array[Any]): Array[Any] = {
    val x: Array[Double] = new Array[Double](row.length)
    row.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = ScoringModelUtils.asDouble(value)
    }
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
    output :+ Field("Score", "Double")
  }

  /**
   * @return metadata about the model
   */
  def modelMetadata(): ModelMetaData = {
    //todo provide an API for the user to populate the custom metadata fields
    new ModelMetaData("Naive Bayes Model", classOf[NaiveBayesModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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
      save(sc, tmpDir.toString)
      ScoringModelUtils.saveToMar(sc, marSavePath, classOf[NaiveBayesModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }

}

/**
 * TK Metadata that will be stored as part of the model
 * @param observationColumns Handle to the observation columns of the data frame
 * @param labelColumn Label column for trained model
 * @param lambdaParameter Smoothing parameter used during model training
 */
case class NaiveBayesModelTkMetaData(observationColumns: List[String],
                                     labelColumn: String,
                                     lambdaParameter: Double) extends Serializable