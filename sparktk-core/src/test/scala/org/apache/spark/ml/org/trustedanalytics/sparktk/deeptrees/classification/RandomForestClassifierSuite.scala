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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.classification

import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.param.ParamsSuite
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree.impl.TreeTests
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.{ MLTestingUtils, DefaultReadWriteTest, MLlibTestSparkContext, SparkFunSuite }
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree.LeafNode
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.EnsembleTestHelper
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.configuration.{ Algo => OldAlgo }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.{ RandomForest => OldRandomForest }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }

/**
 * Test suite for [[RandomForestClassifier]].
 */
class RandomForestClassifierSuite
    extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import RandomForestClassifierSuite.compareAPIs
  //import testImplicits._

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _
  private var orderedLabeledPoints5_20: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000))
    orderedLabeledPoints5_20 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 5, 20))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  def binaryClassificationTestWithContinuousFeatures(rf: RandomForestClassifier) {
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2
    val newRF = rf
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setNumTrees(1)
      .setFeatureSubsetStrategy("auto")
      .setSeed(123)
    compareAPIs(orderedLabeledPoints50_1000, newRF, categoricalFeatures, numClasses)
  }

  test("params") {
    ParamsSuite.checkParams(new RandomForestClassifier)
    val model = new RandomForestClassificationModel("rfc",
      Array(new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 1, 2)), 2, 2)
    ParamsSuite.checkParams(model)
  }

  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
    binaryClassificationTestWithContinuousFeatures(rf)
  }

  test("Binary classification with continuous features and node Id cache:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
      .setCacheNodeIds(true)
    binaryClassificationTestWithContinuousFeatures(rf)
  }

  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0)),
      LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    )
    val rdd = sc.parallelize(arr)
    val categoricalFeatures = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val numClasses = 3

    val rf = new RandomForestClassifier()
      .setImpurity("Gini")
      .setMaxDepth(5)
      .setNumTrees(2)
      .setFeatureSubsetStrategy("sqrt")
      .setSeed(12345)
    compareAPIs(rdd, rf, categoricalFeatures, numClasses)
  }

  test("subsampling rate in RandomForest") {
    val rdd = orderedLabeledPoints5_20
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2

    val rf1 = new RandomForestClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setCacheNodeIds(true)
      .setNumTrees(3)
      .setFeatureSubsetStrategy("auto")
      .setSeed(123)
    compareAPIs(rdd, rf1, categoricalFeatures, numClasses)

    val rf2 = rf1 //.setSubsamplingRate(0.5)
    compareAPIs(rdd, rf2, categoricalFeatures, numClasses)
  }

  test("Fitting without numClasses in metadata") {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val df: DataFrame = TreeTests.featureImportanceData(sc).toDF()
    val rf = new RandomForestClassifier().setMaxDepth(1).setNumTrees(1)
    rf.fit(df)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature importance
  /////////////////////////////////////////////////////////////////////////////

  test("Feature importance with toy data") {
    val numClasses = 2
    val rf = new RandomForestClassifier()
      .setImpurity("Gini")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setFeatureSubsetStrategy("all")
      .setSubsamplingRate(1.0)
      .setSeed(123)

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)

    val importances = rf.fit(df).featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  test("should support all NumericType labels and not support other types") {
    val rf = new RandomForestClassifier().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[RandomForestClassificationModel, RandomForestClassifier](
      rf, sqlContext) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
      model: RandomForestClassificationModel,
      model2: RandomForestClassificationModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
      assert(model.numClasses === model2.numClasses)
    }

    val rf = new RandomForestClassifier().setNumTrees(2)
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("impurity" -> "entropy")

    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)
    testEstimatorAndModelReadWrite(rf, continuousData, allParamSettings, checkModelData)
  }
}

private object RandomForestClassifierSuite extends SparkFunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
    data: RDD[LabeledPoint],
    rf: RandomForestClassifier,
    categoricalFeatures: Map[Int, Int],
    numClasses: Int): Unit = {
    val numFeatures = data.first().features.size
    val oldStrategy =
      rf.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, rf.getOldImpurity)
    val oldModel = OldRandomForest.trainClassifier(
      data, oldStrategy, rf.getNumTrees, rf.getFeatureSubsetStrategy,
      rf.getSeed.toInt)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)
    val newModel = rf.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = RandomForestClassificationModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[RandomForestClassifier], categoricalFeatures,
      numClasses)
    TreeTests.checkEqual(oldModelAsNew, newModel)
    assert(newModel.hasParent)
    assert(!newModel.trees.head.asInstanceOf[DecisionTreeClassificationModel].hasParent)
    assert(newModel.numClasses === numClasses)
    assert(newModel.numFeatures === numFeatures)
  }
}
