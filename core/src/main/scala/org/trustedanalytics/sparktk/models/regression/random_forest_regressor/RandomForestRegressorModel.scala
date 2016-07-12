package org.trustedanalytics.sparktk.models.regression.random_forest_regressor

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.tree.model.{ RandomForestModel => SparkRandomForestModel }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ RowWrapperFunctions, FrameRdd }
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.commons.lang3.StringUtils

import scala.language.implicitConversions
import org.json4s.JsonAST.JValue

object RandomForestRegressorModel extends TkSaveableObject {

  private def getFeatureSubsetCategory(featureSubsetCategory: Option[String], numTrees: Int): String = {
    var value = "all"
    value = featureSubsetCategory.getOrElse("all") match {
      case "auto" =>
        numTrees match {
          case 1 => "all"
          case _ => "onethird"
        }
      case _ => featureSubsetCategory.getOrElse("all")
    }
    value
  }

  /**
   * @param frame The frame containing the data to train on
   * @param valueColumn Column name containing the value for each observation
   * @param observationColumns Column(s) containing the observations
   * @param numTrees Number of tress in the random forest. Default is 1
   * @param impurity Criterion used for information gain calculation. Default supported value is "variance"
   * @param maxDepth Maximum depth of the tree. Default is 4
   * @param maxBins Maximum number of bins used for splitting features. Default is 100
   * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
   * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical
   *                                with 'k' categories indexed from 0:{0,1,...,k-1}
   * @param featureSubsetCategory Number of features to consider for splits at each node.
   *                              Supported values "auto","all","sqrt","log2","onethird".
   *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
   *                              ; if num_trees > 1, set to "onethird"
   */
  def train(frame: Frame,
            valueColumn: String,
            observationColumns: List[String],
            numTrees: Int = 1,
            impurity: String = "variance",
            maxDepth: Int = 4,
            maxBins: Int = 100,
            seed: Int = scala.util.Random.nextInt(),
            categoricalFeaturesInfo: Option[Map[Int, Int]] = None,
            featureSubsetCategory: Option[String] = None): RandomForestRegressorModel = {
    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(valueColumn), "valueColumn must not be null nor empty")
    require(numTrees > 0, "numTrees must be greater than 0")
    require(maxDepth >= 0, "maxDepth must be non negative")
    require(StringUtils.equals(impurity, "variance"), "Only variance is a supported value for impurity " +
      "for Random Forest Regressor model")
    require(featureSubsetCategory.isEmpty ||
      List("auto", "all", "sqrt", "log2", "onethird").contains(featureSubsetCategory.get),
      "feature subset category can be either None or one of the values: auto, all, sqrt, log2, onethird")

    val randomForestFeatureSubsetCategories = getFeatureSubsetCategory(featureSubsetCategory, numTrees)
    val randomForestCategoricalFeaturesInfo = categoricalFeaturesInfo.getOrElse(Map[Int, Int]())

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = FrameRdd.toLabeledPointRDD(new FrameRdd(frame.schema, frame.rdd),
      valueColumn,
      observationColumns)
    val randomForestModel = RandomForest.trainRegressor(labeledTrainRdd,
      randomForestCategoricalFeaturesInfo,
      numTrees,
      randomForestFeatureSubsetCategories,
      impurity,
      maxDepth,
      maxBins,
      seed)

    RandomForestRegressorModel(randomForestModel,
      valueColumn,
      observationColumns,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      seed,
      categoricalFeaturesInfo,
      featureSubsetCategory)
  }

  /**
   * Load a RandomForestRegressorModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): RandomForestRegressorModel = {
    tc.load(path).asInstanceOf[RandomForestRegressorModel]
  }

  override def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: RandomForestRegressorModelTkMetaData = SaveLoad.extractFromJValue[RandomForestRegressorModelTkMetaData](tkMetadata)
    val sparkModel = SparkRandomForestModel.load(sc, path)

    RandomForestRegressorModel(sparkModel,
      m.valueColumn,
      m.observationColumns,
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
 * RandomForestRegressorModel
 * @param sparkModel Trained MLLib's RandomForestRegressor model
 * @param valueColumn Column name containing the value for each observation
 * @param observationColumns Column(s) containing the observations
 * @param numTrees Number of tress in the random forest. Default is 1
 * @param impurity Criterion used for information gain calculation. Default is "variance"
 * @param maxDepth Maximum depth of the tree. Default is 4
 * @param maxBins Maximum number of bins used for splitting features. Default is 100
 * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
 * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical
 *                                with 'k' categories indexed from 0:{0,1,...,k-1}
 * @param featureSubsetCategory Number of features to consider for splits at each node.
 *                              Supported values "auto","all","sqrt","log2","onethird".
 *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
 *                              ; if num_trees > 1, set to "onethird"
 */
case class RandomForestRegressorModel private[random_forest_regressor] (sparkModel: SparkRandomForestModel,
                                                                        valueColumn: String,
                                                                        observationColumns: List[String],
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
   *                By default, we predict the labels over columns the RandomForestRegressorModel
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
      val prediction = sparkModel.predict(point)
      Row.apply(prediction)
    }

    frame.addColumns(predictMapper, Seq(Column("predicted_value", DataTypes.float64)))
  }

  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(sc, path)
    val formatVersion: Int = 1
    val tkMetadata = RandomForestRegressorModelTkMetaData(valueColumn,
      observationColumns,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      seed,
      categoricalFeaturesInfo,
      featureSubsetCategory)
    TkSaveLoad.saveTk(sc, path, RandomForestRegressorModel.formatId, formatVersion, tkMetadata)
  }
}

/**
 * TK Metadata that will be stored as part of the model
 * @param valueColumn Column name containing the value for each observation
 * @param observationColumns Column(s) containing the observations
 * @param numTrees Number of tress in the random forest
 * @param impurity Criterion used for information gain calculation. Default supported value is "variance"
 * @param maxDepth Maximum depth of the tree. Default is 4
 * @param maxBins Maximum number of bins used for splitting features
 * @param seed Random seed for bootstrapping and choosing feature subsets
 * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical
 *                                with 'k' categories indexed from 0:{0,1,...,k-1}
 * @param featureSubsetCategory Number of features to consider for splits at each node
 */
case class RandomForestRegressorModelTkMetaData(valueColumn: String,
                                                observationColumns: List[String],
                                                numTrees: Int,
                                                impurity: String,
                                                maxDepth: Int,
                                                maxBins: Int,
                                                seed: Int,
                                                categoricalFeaturesInfo: Option[Map[Int, Int]],
                                                featureSubsetCategory: Option[String]) extends Serializable