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
package org.trustedanalytics.sparktk.models.classification.random_forest_classifier

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.classification.{ RandomForestClassifier => SparkDeepRandomForestClassifier }
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.classification.{ RandomForestClassificationModel => SparkDeepRandomClassificationModel }
import org.trustedanalytics.sparktk.TkContext
import org.trustedanalytics.sparktk.frame._
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricsFunctions, ClassificationMetricValue }
import org.trustedanalytics.sparktk.frame.internal.rdd.{ ScoreAndLabel, RowWrapperFunctions, FrameRdd }
import org.trustedanalytics.sparktk.models.regression.RegressionColumnNames._
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

object RandomForestClassifierModel extends TkSaveableObject {

  private def getFeatureSubsetCategory(featureSubsetCategory: String, numTrees: Int): String = {
    featureSubsetCategory match {
      case "auto" =>
        numTrees match {
          case 1 => "all"
          case _ => "sqrt"
        }
      case x => x
    }
  }

  /**
   * Train a RandomForestClassifierModel
   * @param frame The frame containing the data to train on
   * @param observationColumns Column(s) containing the observations
   * @param labelColumn Column name containing the label for each observation
   * @param numClasses Number of classes for classification. Default is 2
   *                   numClasses should not exceed the number of distinct values in labelColumn
   * @param numTrees Number of tress in the random forest. Default is 1
   * @param impurity Criterion used for information gain calculation. Supported values "gini" or "entropy".
   *                 Default is "gini"
   * @param maxDepth Maximum depth of the tree. Default is 4
   * @param maxBins Maximum number of bins used for splitting features. Default is 100
   * @param minInstancesPerNode Minimum number of records each child node must have after a split. Default is 1
   * @param subSamplingRate Fraction of the training data used for learning each decision tree. Default is 1.0
   * @param featureSubsetCategory Subset of observation columns, i.e., features, to consider when looking for the best split.
   *                              Supported values "auto","all","sqrt","log2","onethird".
   *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
   *                              ; if num_trees > 1, set to "sqrt". Default is "auto"
   * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
   * @param categoricalFeaturesInfo Optional arity of categorical features. Entry (name -> k) indicates that feature
   *                                'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}
   */
  def train(frame: Frame,
            observationColumns: List[String],
            labelColumn: String,
            numClasses: Int = 2,
            numTrees: Int = 1,
            impurity: String = "gini",
            maxDepth: Int = 4,
            maxBins: Int = 100,
            minInstancesPerNode: Int = 1,
            subSamplingRate: Double = 1.0,
            featureSubsetCategory: String = "auto",
            seed: Int = scala.util.Random.nextInt(),
            categoricalFeaturesInfo: Option[Map[String, Int]] = None): RandomForestClassifierModel = {
    require(frame != null, "frame is required")
    require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
    require(StringUtils.isNotEmpty(labelColumn), "labelColumn must not be null nor empty")
    require(numTrees > 0, "numTrees must be greater than 0")
    require(maxDepth >= 0, "maxDepth must be non negative")
    require(numClasses >= 2, "numClasses must be at least 2")
    require(featureSubsetCategory.isEmpty ||
      List("auto", "all", "sqrt", "log2", "onethird").contains(featureSubsetCategory),
      "feature subset category can be either None or one of the values: auto, all, sqrt, log2, onethird")
    require(List("gini", "entropy").contains(impurity), "Supported values for impurity are gini or entropy")
    require(minInstancesPerNode > 0, "minInstancesPerNode must be greater than 0")
    require(subSamplingRate > 0 && subSamplingRate <= 1, "subSamplingRate must be in range (0, 1]")
    require(maxBins > 0, "maxBins must be greater than 0")
    frame.schema.validateColumnsExist(observationColumns :+ labelColumn)

    val randomForestFeatureSubsetCategories = getFeatureSubsetCategory(featureSubsetCategory, numTrees)

    //create RDD from the frame
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val trainFrame = frameRdd.toLabeledDataFrame(observationColumns, labelColumn,
      featuresColName, categoricalFeaturesInfo, Some(numClasses))

    val randomForestClassifier = new SparkDeepRandomForestClassifier()
      .setNumTrees(numTrees)
      .setFeatureSubsetStrategy(randomForestFeatureSubsetCategories)
      .setImpurity(impurity)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setSeed(seed)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setSubsamplingRate(subSamplingRate)
      .setLabelCol(labelColumn)
      .setFeaturesCol(featuresColName)
      .setCacheNodeIds(true) //Enable cache to speed up training
    val randomForestModel = randomForestClassifier.fit(trainFrame)

    RandomForestClassifierModel(randomForestModel,
      observationColumns,
      labelColumn,
      numClasses,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      minInstancesPerNode,
      subSamplingRate,
      featureSubsetCategory,
      seed,
      categoricalFeaturesInfo
    )
  }

  /**
   * Load a RandomForestClassifierModel from the given path
   * @param tc TkContext
   * @param path location
   * @return
   */
  def load(tc: TkContext, path: String): RandomForestClassifierModel = {
    tc.load(path).asInstanceOf[RandomForestClassifierModel]
  }

  override def loadTkSaveableObject(sc: SparkContext, path: String, formatVersion: Int, tkMetadata: JValue): Any = {

    validateFormatVersion(formatVersion, 1)
    val m: RandomForestClassifierModelTkMetaData = SaveLoad.extractFromJValue[RandomForestClassifierModelTkMetaData](tkMetadata)
    val sparkModel = SparkDeepRandomClassificationModel.load(path)

    RandomForestClassifierModel(sparkModel,
      m.observationColumns,
      m.labelColumn,
      m.numClasses,
      m.numTrees,
      m.impurity,
      m.maxDepth,
      m.maxBins,
      m.minInstancesPerNode,
      m.subSamplingRate,
      m.featureSubsetCategory,
      m.seed,
      m.categoricalFeaturesInfo
    )
  }
}

/**
 * RandomForestClassifierModel
 * @param sparkModel Trained MLLib's RandomForestClassifier model
 * @param observationColumns Column(s) containing the observations
 * @param labelColumn Column name containing the label for each observation
 * @param numClasses Number of classes for classification. Default is 2
 * @param numTrees Number of tress in the random forest. Default is 1
 * @param impurity Criterion used for information gain calculation. Supported values "gini" or "entropy".
 *                 Default is "gini"
 * @param maxDepth Maximum depth of the tree. Default is 4
 * @param maxBins Maximum number of bins used for splitting features. Default is 100
 * @param minInstancesPerNode Minimum number of records each child node must have after a split. Default is 1
 * @param subSamplingRate Fraction of the training data used for learning each decision tree. Default is 1.0
 * @param featureSubsetCategory Subset of observation columns, i.e., features, to consider when looking for the best split.
 *                              Supported values "auto","all","sqrt","log2","onethird".
 *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
 *                              ; if num_trees > 1, set to "sqrt".
 * @param seed Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed
 * @param categoricalFeaturesInfo Optional arity of categorical features. Entry (n-> k) indicates that feature 'n' is name of
 *                                categorical feature with 'k' categories indexed from 0:{0,1,...,k-1}
 */
case class RandomForestClassifierModel private[random_forest_classifier] (sparkModel: SparkDeepRandomClassificationModel,
                                                                          observationColumns: List[String],
                                                                          labelColumn: String,
                                                                          numClasses: Int,
                                                                          numTrees: Int,
                                                                          impurity: String,
                                                                          maxDepth: Int,
                                                                          maxBins: Int,
                                                                          minInstancesPerNode: Int,
                                                                          subSamplingRate: Double,
                                                                          featureSubsetCategory: String,
                                                                          seed: Int,
                                                                          categoricalFeaturesInfo: Option[Map[String, Int]]) extends Serializable with Model {

  implicit def rowWrapperToRowWrapperFunctions(rowWrapper: RowWrapper): RowWrapperFunctions = {
    new RowWrapperFunctions(rowWrapper)
  }

  /**
   * Predict the labels for a test frame using trained Random Forest Classifier model, and create a new frame revision
   * with existing columns and a new predicted label’s column.
   *
   * @param frame - frame to add predictions to
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted.
   *                By default, the same observation column names from training are used
   * @return A new frame consisting of the existing columns of the frame and a new column with predicted label
   *         for each observation.
   */
  def predict(frame: Frame, observationColumns: Option[List[String]] = None): Frame = {
    require(frame != null, "frame is required")
    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and predict should be same")
    }

    val observations = observationColumns.getOrElse(this.observationColumns)
    frame.schema.validateColumnsExist(observations)
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val trainFrame = frameRdd.toLabeledDataFrame(observations, labelColumn,
      featuresColName, labelNumClasses = Some(numClasses))
    val assembler = new VectorAssembler().setInputCols(observations.toArray).setOutputCol(featuresColName)
    val testFrame = assembler.transform(frame.dataframe)

    sparkModel.setFeaturesCol(featuresColName)
    sparkModel.setPredictionCol("predicted_class")
    val toIntUdf = udf { s: Double => s.toInt }
    val predictFrame = sparkModel.transform(testFrame).withColumn("predicted_class", toIntUdf(col("predicted_class")))

    new Frame(predictFrame.drop(col(featuresColName)))
  }

  /**
   * Get the predictions for observations in a test frame
   *
   * @param frame Frame to test the RandomForestClassifier model
   * @param observationColumns Column(s) containing the observations whose labels are to be predicted.
   *                By default, the same observation column names from training are used
   * @param labelColumn Column name containing the label for each observation
   *                By default, the same label column name from training is used
   * @return ClassificationMetricValue describing the test metrics
   */
  def test(frame: Frame, observationColumns: Option[List[String]] = None, labelColumn: Option[String] = None): ClassificationMetricValue = {

    if (observationColumns.isDefined) {
      require(observationColumns.get.length == this.observationColumns.length, "Number of columns for train and test should be same")
    }
    val observations = observationColumns.getOrElse(this.observationColumns)
    val label = labelColumn.getOrElse(this.labelColumn)
    frame.schema.validateColumnsExist(observations :+ label)
    //predicting and testing
    val frameRdd = new FrameRdd(frame.schema, frame.rdd)
    val scoreAndLabelRdd = frameRdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(observations, label)
      val score = sparkModel.predict(labeledPoint.features)
      ScoreAndLabel(score, labeledPoint.label)
    })

    val output: ClassificationMetricValue = numClasses match {
      case 2 =>
        val posLabel = 1d
        ClassificationMetricsFunctions.binaryClassificationMetrics(scoreAndLabelRdd, posLabel)
      case _ => ClassificationMetricsFunctions.multiclassClassificationMetrics(scoreAndLabelRdd)
    }
    output
  }

  /**
   * Feature importances for trained model. Higher values indicate more important features.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1.
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
   * @param overwrite Boolean indicating if the directory will be overwritten, if it already exists.
   */
  def save(sc: SparkContext, path: String, overwrite: Boolean = false): Unit = {
    if (overwrite)
      sparkModel.write.overwrite().save(path)
    else
      sparkModel.write.save(path)
    val formatVersion: Int = 1
    val tkMetadata = RandomForestClassifierModelTkMetaData(observationColumns,
      labelColumn,
      numClasses,
      numTrees,
      impurity,
      maxDepth,
      maxBins,
      minInstancesPerNode,
      subSamplingRate,
      featureSubsetCategory,
      seed,
      categoricalFeaturesInfo)
    TkSaveLoad.saveTk(sc, path, RandomForestClassifierModel.formatId, formatVersion, tkMetadata)
  }

  override def score(data: Array[Any]): Array[Any] = {
    require(data != null && data.length > 0, "scoring data array should not be null nor empty")
    val x: Array[Double] = new Array[Double](data.length)
    data.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = ScoringModelUtils.asDouble(value)
    }
    data :+ sparkModel.predict(Vectors.dense(x))
  }

  override def modelMetadata(): ModelMetaData = {
    new ModelMetaData("Random Forest Classifier Model", classOf[RandomForestClassifierModel].getName, classOf[SparkTkModelAdapter].getName, Map())
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
    output :+ Field("PredictedClass", "Double")
  }

  def exportToMar(sc: SparkContext, marSavePath: String): String = {
    var tmpDir: Path = null
    try {
      tmpDir = Files.createTempDirectory("sparktk-scoring-model")
      save(sc, tmpDir.toString, overwrite = true)
      ScoringModelUtils.saveToMar(marSavePath, classOf[RandomForestClassifierModel].getName, tmpDir)
    }
    finally {
      sys.addShutdownHook(FileUtils.deleteQuietly(tmpDir.toFile)) // Delete temporary directory on exit
    }
  }
}

/**
 * TK Metadata that will be stored as part of the model
 * @param observationColumns Column(s) containing the observations
 * @param labelColumn Column name containing the label for each observation
 * @param numClasses Number of classes for classification
 * @param numTrees Number of tress in the random forest
 * @param impurity Criterion used for information gain calculation. Supported values "gini" or "entropy".
 * @param maxDepth Maximum depth of the tree. Default is 4
 * @param maxBins Maximum number of bins used for splitting features
 * @param minInstancesPerNode Minimum number of records each child node must have after a split. Default is 1
 * @param subSamplingRate Fraction of the training data used for learning each decision tree. Default is 1.0
 * @param featureSubsetCategory Subset of observation columns, i.e., features, to consider when looking for the best split.
 *                              Supported values are "auto","all","sqrt","log2","onethird".
 *                              If "auto" is set, this is based on num_trees: if num_trees == 1, set to "all"
 *                              ; if num_trees > 1, set to "sqrt"
 * @param seed Random seed for bootstrapping and choosing feature subsets
 * @param categoricalFeaturesInfo Optional arity of categorical features. Entry (name -> k) indicates that feature
 *                                'name' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}
 */
case class RandomForestClassifierModelTkMetaData(observationColumns: List[String],
                                                 labelColumn: String,
                                                 numClasses: Int,
                                                 numTrees: Int,
                                                 impurity: String,
                                                 maxDepth: Int,
                                                 maxBins: Int,
                                                 minInstancesPerNode: Int,
                                                 subSamplingRate: Double,
                                                 featureSubsetCategory: String,
                                                 seed: Int,
                                                 categoricalFeaturesInfo: Option[Map[String, Int]]) extends Serializable