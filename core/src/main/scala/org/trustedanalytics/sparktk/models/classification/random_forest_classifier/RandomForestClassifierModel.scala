package org.trustedanalytics.sparktk.models.classification.random_forest_classifier

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.tree.model.{ RandomForestModel => SparkRandomForestModel }
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ClassificationMetricsFunctions, ClassificationMetricValue}
import org.trustedanalytics.sparktk.frame.internal.rdd.{ScoreAndLabel, RowWrapperFunctions, FrameRdd}
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.commons.lang3.StringUtils

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object RandomForestClassifierModel extends TkSaveableObject {

  def getFeatureSubsetCategory(featureSubsetCategory: Option[String], numTrees: Int): String = {
    var value = "all"
    value = featureSubsetCategory.getOrElse("all") match {
      case "auto" =>
        numTrees match {
          case 1 => "all"
          case _ => "sqrt"
        }
      case x => x
    }
    value
  }

  /**
   * @param frame The frame containing the data to train on
   * @param labelColumn Column name containing the label for each observation
   * @param observationColumns Column(s) containing the observations
   * @param numClasses Number of classes for classification. Default is 2
   * @param numTrees Number of tress in the random forest. Default is 1
   * @param impurity Criterion used for information gain calculation. Supported values "gini" or "entropy".
   *                 Default is "gini"
   * @param maxDepth Maximum depth of the tree. Default is 4
   * @param maxBins Maximum number of bins used for splitting features. Default is 100
   * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
   * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical
   *                                with 'k' categories indexed from 0:{0,1,...,k-1}
   * @param featureSubsetCategory Number of features to consider for splits at each node.
   *                              Supported values "auto","all","sqrt","log2","onethird".
   *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
   *                              ; if num_trees > 1, set to "sqrt"
   */
  def train(frame: Frame,
            labelColumn: String,
            observationColumns: List[String],
            numClasses: Int = 2,
            numTrees: Int = 1,
            impurity: String = "gini",
            maxDepth: Int = 4,
            maxBins: Int = 100,
            seed: Int = scala.util.Random.nextInt(),
            categoricalFeaturesInfo: Option[Map[Int, Int]] = None,
            featureSubsetCategory: Option[String] = None): RandomForestClassifierModel = {
    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(labelColumn), "labelColumn must not be null nor empty")

    val randomForestFeatureSubsetCategories = getFeatureSubsetCategory(featureSubsetCategory, numTrees)
    val randomForestCategoricalFeaturesInfo = categoricalFeaturesInfo.getOrElse(Map[Int, Int]())

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = FrameRdd.toLabeledPointRDD(new FrameRdd(frame.schema, frame.rdd), labelColumn, observationColumns)
    val randomForestModel = RandomForest.trainClassifier(labeledTrainRdd, numClasses, randomForestCategoricalFeaturesInfo, numTrees,
      randomForestFeatureSubsetCategories, impurity, maxDepth, maxBins, seed)

    RandomForestClassifierModel(randomForestModel, labelColumn, observationColumns, numClasses,
      numTrees, impurity, maxDepth, maxBins, seed, categoricalFeaturesInfo, featureSubsetCategory)
  }


  def load(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: RandomForestClassifierModelTkMetaData = SaveLoad.extractFromJValue[RandomForestClassifierModelTkMetaData](tkMetadata)
    val sparkModel = SparkRandomForestModel.load(sc, path)

    RandomForestClassifierModel(sparkModel,
      m.labelColumn,
      m.observationColumns,
      m.numClasses,
      m.numTrees,
      m.impurity,
      m.maxDepth,
      m.maxBins,
      m.seed,
      m.categoricalFeaturesInfo,
      m.featureSubsetCategory)
  }
}

/**
 * NaiveBayesModel
 * @param sparkModel Trained MLLib's RandomForestClassifier model
 * @param labelColumn Column name containing the label for each observation
 * @param observationColumns Column(s) containing the observations
 * @param numClasses Number of classes for classification. Default is 2
 * @param numTrees Number of tress in the random forest. Default is 1
 * @param impurity Criterion used for information gain calculation. Supported values "gini" or "entropy".
 *                 Default is "gini"
 * @param maxDepth Maximum depth of the tree. Default is 4
 * @param maxBins Maximum number of bins used for splitting features. Default is 100
 * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
 * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical
 *                                with 'k' categories indexed from 0:{0,1,...,k-1}
 * @param featureSubsetCategory Number of features to consider for splits at each node.
 *                              Supported values "auto","all","sqrt","log2","onethird".
 *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
 *                              ; if num_trees > 1, set to "sqrt"
 */
case class RandomForestClassifierModel private[random_forest_classifier] (sparkModel: SparkRandomForestModel,
                                                                        labelColumn: String,
                                                                        observationColumns: List[String],
                                                                        numClasses: Int,
                                                                        numTrees: Int,
                                                                        impurity: String,
                                                                        maxDepth: Int,
                                                                        maxBins: Int,
                                                                        seed: Int,
                                                                        categoricalFeaturesInfo: Option[Map[Int, Int]],
                                                                        featureSubsetCategory: Option[String]) extends Serializable {

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

    val rfColumns = columns.getOrElse(observationColumns)
    //predicting a label for the observation columns
    val predictMapper: RowWrapper => Row = row => {
      val point = row.toDenseVector(rfColumns)
      val prediction = sparkModel.predict(point).toInt
      Row.apply(prediction)
    }

    frame.addColumns(predictMapper, Seq(Column("predicted_class", DataTypes.int32)))
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame Frame to test the RandomForestClassifier model
   * @param columns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the NaiveBayesModel
   * @return ClassificationMetricValue describing the test metrics
   */
  def test(frame: Frame, columns: Option[List[String]]): ClassificationMetricValue = {

    if (columns.isDefined) {
      require(columns.get.length == observationColumns.length, "Number of columns for train and test should be same")
    }
    val rfColumns = columns.getOrElse(observationColumns)

    //predicting and testing
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(rfColumns, labelColumn)
      val score = sparkModel.predict(labeledPoint.features)
      ScoreAndLabel(score, labeledPoint.label)
    })

    val output: ClassificationMetricValue = numClasses match {
      case 2 => val posLabel = 1d
        ClassificationMetricsFunctions.binaryClassificationMetrics(scoreAndLabelRdd, posLabel)
      case _ => ClassificationMetricsFunctions.multiclassClassificationMetrics(scoreAndLabelRdd)
    }
    output
  }


  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path)
    val formatVersion: Int = 1
    val tkMetadata = RandomForestClassifierModelTkMetaData(labelColumn,
      observationColumns,
      numClasses,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      seed,
      categoricalFeaturesInfo,
      featureSubsetCategory)
    TkSaveLoad.saveTk(sc, path, RandomForestClassifierModel.formatId, formatVersion, tkMetadata)
  }
}

/**
 * TK Metadata that will be stored as part of the model
 * @param labelColumn Column name containing the label for each observation
 * @param observationColumns Column(s) containing the observations
 * @param numClasses Number of classes for classification
 * @param numTrees Number of tress in the random forest
 * @param impurity Criterion used for information gain calculation. Supported values "gini" or "entropy".
 * @param maxDepth Maximum depth of the tree. Default is 4
 * @param maxBins Maximum number of bins used for splitting features
 * @param seed Random seed for bootstrapping and choosing feature subsets
 * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical
 *                                with 'k' categories indexed from 0:{0,1,...,k-1}
 * @param featureSubsetCategory Number of features to consider for splits at each node
 */
case class RandomForestClassifierModelTkMetaData(labelColumn: String,
                                                 observationColumns: List[String],
                                                 numClasses: Int,
                                                 numTrees: Int,
                                                 impurity: String,
                                                 maxDepth: Int,
                                                 maxBins: Int,
                                                 seed: Int,
                                                 categoricalFeaturesInfo: Option[Map[Int, Int]],
                                                 featureSubsetCategory: Option[String]) extends Serializable