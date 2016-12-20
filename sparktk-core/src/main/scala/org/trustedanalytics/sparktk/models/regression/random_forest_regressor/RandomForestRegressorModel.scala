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
package org.trustedanalytics.sparktk.models.regression.random_forest_regressor

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.regression.{ RandomForestRegressor => SparkDeepRandomForestRegressor }
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.regression.{ RandomForestRegressionModel => SparkDeepRandomRegressionModel }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.rdd.{ RowWrapperFunctions, FrameRdd }
import org.trustedanalytics.sparktk.models.regression.RegressionUtils._
import org.trustedanalytics.sparktk.saveload.{ SaveLoad, TkSaveLoad, TkSaveableObject }
import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.scoring.interfaces.{ ModelMetaData, Field, Model }
import org.trustedanalytics.sparktk.models.{ FrameImplicits, SparkTkModelAdapter, ScoringModelUtils }
import scala.language.implicitConversions
import org.json4s.JsonAST.JValue
import org.apache.spark.mllib.linalg.Vectors
import java.nio.file.{ Files, Path }
import org.apache.commons.io.FileUtils
import FrameImplicits._
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
   * Train a RandomForestRegressorModel
   * @param frame The frame containing the data to train on
   * @param valueColumn Column name containing the value for each observation
   * @param observationColumns Column(s) containing the observations
   * @param numTrees Number of tress in the random forest. Default is 1
   * @param impurity Criterion used for information gain calculation. Default supported value is "variance"
   * @param maxDepth Maximum depth of the tree. Default is 4
   * @param maxBins Maximum number of bins used for splitting features. Default is 100
   * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
   * @param categoricalFeaturesInfo Arity of categorical features. Entry (name -> k) indicates that feature
   *                                'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}
   * @param featureSubsetCategory Number of features to consider for splits at each node.
   *                              Supported values "auto","all","sqrt","log2","onethird".
   *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
   *                              ; if num_trees > 1, set to "onethird"
   * @param minInstancesPerNode Minimum number of instances each child must have after split. Default is 1
   * @param subSamplingRate Fraction of the training data used for learning each decision tree. Default is 1.0
   */
  def train(frame: Frame,
            valueColumn: String,
            observationColumns: List[String],
            numTrees: Int = 1,
            impurity: String = "variance",
            maxDepth: Int = 4,
            maxBins: Int = 100,
            seed: Int = scala.util.Random.nextInt(),
            categoricalFeaturesInfo: Option[Map[String, Int]] = None,
            featureSubsetCategory: Option[String] = None,
            minInstancesPerNode: Option[Int] = Some(1),
            subSamplingRate: Option[Double] = Some(1.0)): RandomForestRegressorModel = {
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
    require(minInstancesPerNode.isEmpty || minInstancesPerNode.get > 0, "minInstancesPerNode must be greater than 0")
    require(subSamplingRate.isEmpty || (subSamplingRate.get > 0 && subSamplingRate.get <= 1),
      "subSamplingRate must be in range (0, 1]")

    val randomForestFeatureSubsetCategories = getFeatureSubsetCategory(featureSubsetCategory, numTrees)
    val randomForestMinInstancesPerNode = minInstancesPerNode.getOrElse(1)
    val randomForestSubSamplingRate = subSamplingRate.getOrElse(1.0)

    //create RDD from the frame
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val trainFrame = frameRdd.toLabeledDataFrame(valueColumn, observationColumns,
      featuresName, categoricalFeaturesInfo)

    val randomForestRegressor = new SparkDeepRandomForestRegressor()
      .setNumTrees(numTrees)
      .setFeatureSubsetStrategy(randomForestFeatureSubsetCategories)
      .setImpurity(impurity)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setSeed(seed)
      .setMinInstancesPerNode(randomForestMinInstancesPerNode)
      .setSubsamplingRate(randomForestSubSamplingRate)
      .setLabelCol(valueColumn)
      .setFeaturesCol(featuresName)
      .setCacheNodeIds(true) //Enable cache to speed up training
    val randomForestModel = randomForestRegressor.fit(trainFrame)

    RandomForestRegressorModel(randomForestModel,
      valueColumn,
      observationColumns,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      seed,
      categoricalFeaturesInfo,
      featureSubsetCategory,
      minInstancesPerNode,
      subSamplingRate)
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
    val sparkModel = SparkDeepRandomRegressionModel.load(path)

    RandomForestRegressorModel(sparkModel,
      m.valueColumn,
      m.observationColumns,
      m.numTrees,
      m.impurity,
      m.maxDepth,
      m.maxBins,
      m.seed,
      m.categoricalFeaturesInfo,
      m.featureSubsetCategory,
      m.minInstancesPerNode,
      m.subSamplingRate
    )
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
 * @param categoricalFeaturesInfo Arity of categorical features. Entry (n-> k) indicates that feature 'n' is name of
 *                                categorical feature with 'k' categories indexed from 0:{0,1,...,k-1}
 * @param featureSubsetCategory Number of features to consider for splits at each node.
 *                              Supported values "auto","all","sqrt","log2","onethird".
 *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
 *                              ; if num_trees > 1, set to "onethird"
 * @param minInstancesPerNode Minimum number of instances each child must have after split. Default is 1
 * @param subSamplingRate Fraction of the training data used for learning each decision tree. Default is 1.0
 */
case class RandomForestRegressorModel private[random_forest_regressor] (sparkModel: SparkDeepRandomRegressionModel,
                                                                        valueColumn: String,
                                                                        observationColumns: List[String],
                                                                        numTrees: Int,
                                                                        impurity: String,
                                                                        maxDepth: Int,
                                                                        maxBins: Int,
                                                                        seed: Int,
                                                                        categoricalFeaturesInfo: Option[Map[String, Int]],
                                                                        featureSubsetCategory: Option[String],
                                                                        minInstancesPerNode: Option[Int],
                                                                        subSamplingRate: Option[Double]) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Predict the values for the data points.
   *
   * Predict the values for a test frame using trained Random Forest Classifier model, and create a new frame revision
   * with existing columns and a new predicted value’s column.
   *
   * @param frame - A frame whose labels are to be predicted. By default, predict is run on the same columns over which
   *              the model is trained.
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted.
   *                By default, we predict the labels over columns the RandomForestRegressorModel
   * @return A new frame consisting of the existing columns of the frame and a new column with predicted value for
   *         each observation.
   */
  def predict(frame: Frame, observationColumns: Option[List[String]] = None): Frame = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and predict should be same")
    }

    val rfColumns = observationColumns.getOrElse(this.observationColumns)
    val assembler = new VectorAssembler().setInputCols(rfColumns.toArray).setOutputCol(featuresName)
    val testFrame = assembler.transform(frame.dataframe)

    sparkModel.setFeaturesCol(featuresName)
    sparkModel.setPredictionCol(predictionColumn)
    val predictFrame = sparkModel.transform(testFrame)

    new Frame(predictFrame.drop(col(featuresName)))
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame                  Frame to test the random forest regression model on
   * @param valueColumn            Column name containing the value of each observation
   * @param observationColumns List of column(s) containing the observations
   * @return regression metrics
   *         The data returned is composed of the following:
   *         'explainedVariance' : double
   *         The explained variance regression score.
   *         'meanAbsoluteError' : double
   *         The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
   *         'meanSquaredError': double
   *         The risk function corresponding to the expected value of the squared error loss or quadratic loss
   *         'r2' : double
   *         The coefficient of determination
   *         'rootMeanSquaredError' : double
   *         The square root of the mean squared error
   *         'rootMeanSquaredError' : double
   *         The square root of the mean squared error
   */
  def test(frame: Frame, valueColumn: String, observationColumns: Option[List[String]] = None) = {
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and predict should be same")
    }

    val rfColumns = observationColumns.getOrElse(this.observationColumns)
    val assembler = new VectorAssembler().setInputCols(rfColumns.toArray).setOutputCol(featuresName)
    val testFrame = assembler.transform(frame.dataframe)

    sparkModel.setFeaturesCol(featuresName)
    sparkModel.setPredictionCol(predictionColumn)
    val predictFrame = sparkModel.transform(testFrame)

    getRegressionMetrics(predictFrame, predictionColumn, valueColumn)
  }

  /**
   * Feature importances for trained model. Higher values indicate more important features.
   *
   * @return Map of feature names and importances.
   */
  def featureImportances(): Map[String, Double] = {
    val featureImportances = sparkModel.featureImportances
    observationColumns.zip(featureImportances.toArray).toMap
  }

  /**
   * Saves this model to a file
   * @param sc active SparkContext
   * @param path save to path
   */
  def save(sc: SparkContext, path: String): Unit = {
    sparkModel.save(path)
    val formatVersion: Int = 1
    val tkMetadata = RandomForestRegressorModelTkMetaData(valueColumn,
      observationColumns,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      seed,
      categoricalFeaturesInfo,
      featureSubsetCategory,
      minInstancesPerNode,
      subSamplingRate)
    TkSaveLoad.saveTk(sc, path, RandomForestRegressorModel.formatId, formatVersion, tkMetadata)
  }

  override def score(data: Array[Any]): Array[Any] = {
    require(data != null && data.length > 0, "scoring data must not be null or empty.")
    val x: Array[Double] = new Array[Double](data.length)
    data.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = ScoringModelUtils.asDouble(value)
    }
    data :+ sparkModel.predict(Vectors.dense(x))
  }

  override def modelMetadata(): ModelMetaData = {
    new ModelMetaData("Random Forest Regressor Model", classOf[RandomForestRegressorModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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
      save(sc, tmpDir.toString)
      ScoringModelUtils.saveToMar(marSavePath, classOf[RandomForestRegressorModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
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
 * @param minInstancesPerNode Minimum number of instances each child must have after split. Default is 1
 * @param subSamplingRate Fraction of the training data used for learning each decision tree. Default is 1.0
 */
case class RandomForestRegressorModelTkMetaData(valueColumn: String,
                                                observationColumns: List[String],
                                                numTrees: Int,
                                                impurity: String,
                                                maxDepth: Int,
                                                maxBins: Int,
                                                seed: Int,
                                                categoricalFeaturesInfo: Option[Map[String, Int]],
                                                featureSubsetCategory: Option[String],
                                                minInstancesPerNode: Option[Int],
                                                subSamplingRate: Option[Double]) extends Serializable