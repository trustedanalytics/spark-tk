package org.trustedanalytics.sparktk.models.classification.naive_bayes

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{ NaiveBayesModel => SparkNaiveBayesModel, NaiveBayes }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricsFunctions, ClassificationMetricValue }
import org.trustedanalytics.sparktk.frame.internal.rdd.{ RowWrapperFunctions, ScoreAndLabel, FrameRdd }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.commons.lang3.StringUtils

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
            labelColumn: String,
            observationColumns: Seq[String],
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
    NaiveBayesModel(naiveBayesModel, labelColumn, observationColumns, lambdaParameter)
  }

  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: NaiveBayesModelTkMetaData = SaveLoad.extractFromJValue[NaiveBayesModelTkMetaData](tkMetadata)
    val sparkModel = SparkNaiveBayesModel.load(sc, path)

    NaiveBayesModel(sparkModel, m.labelColumn, m.observationColumns, m.lambdaParameter)
  }
}

/**
 * NaiveBayesModel
 * @param sparkModel Trained MLLib's Naive Bayes model
 * @param labelColumn Label column for trained model
 * @param observationColumns Handle to the observation columns of the data frame
 * @param lambdaParameter Smoothing parameter used during model training
 */
case class NaiveBayesModel private[naive_bayes] (sparkModel: SparkNaiveBayesModel,
                                                 labelColumn: String,
                                                 observationColumns: Seq[String],
                                                 lambdaParameter: Double) extends Serializable {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Adds a column to the frame which indicates the predicted class for each observation
   * @param frame - frame to add predictions to
   * @param columns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the NaiveBayesModel
   */
  def predict(frame: Frame, columns: Option[List[String]] = None): Unit = {
    require(frame != null, "frame is required")
    if (columns.isDefined) {
      require(columns.get.length == observationColumns.length, "Number of columns for train and predict should be same")
    }

    val naiveBayesColumns = columns.getOrElse(observationColumns)
    val predictMapper: RowWrapper => Row = row => {
      val point = row.toDenseVector(naiveBayesColumns)
      val prediction = sparkModel.predict(point)
      Row.apply(prediction)
    }

    frame.addColumns(predictMapper, Seq(Column("predicted_class", DataTypes.float64)))
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame Frame to test the NaiveBayes model
   * @param columns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the NaiveBayesModel
   * @return ClassificationMetricValue describing the test metrics
   */
  def test(frame: Frame, columns: Option[List[String]]): ClassificationMetricValue = {

    if (columns.isDefined) {
      require(columns.get.length == observationColumns.length, "Number of columns for train and test should be same")
    }
    val naiveBayesColumns = columns.getOrElse(observationColumns)

    //predicting and testing
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(naiveBayesColumns, labelColumn)
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
    val tkMetadata = NaiveBayesModelTkMetaData(labelColumn, observationColumns, lambdaParameter)
    TkSaveLoad.saveTk(sc, path, NaiveBayesModel.formatId, formatVersion, tkMetadata)
  }
}

/**
 * TK Metadata that will be stored as part of the model
 * @param labelColumn Label column for trained model
 * @param observationColumns Handle to the observation columns of the data frame
 * @param lambdaParameter Smoothing parameter used during model training
 */
case class NaiveBayesModelTkMetaData(labelColumn: String, observationColumns: Seq[String], lambdaParameter: Double) extends Serializable