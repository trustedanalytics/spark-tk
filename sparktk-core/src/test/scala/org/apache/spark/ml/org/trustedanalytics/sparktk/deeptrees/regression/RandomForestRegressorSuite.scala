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
package org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.regression

import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.tree.impl.TreeTests
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util.{ DefaultReadWriteTest, MLlibTestSparkContext, SparkFunSuite }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.EnsembleTestHelper
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.configuration.{ Algo => OldAlgo }
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.{ RandomForest => OldRandomForest }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Test suite for [[RandomForestRegressor]].
 */
class RandomForestRegressorSuite extends SparkFunSuite with MLlibTestSparkContext
    with DefaultReadWriteTest {

  import RandomForestRegressorSuite.compareAPIs

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  def regressionTestWithContinuousFeatures(rf: RandomForestRegressor) {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val newRF = rf
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(10)
      .setNumTrees(1)
      .setFeatureSubsetStrategy("auto")
      .setSeed(123)
    compareAPIs(orderedLabeledPoints50_1000, newRF, categoricalFeaturesInfo)
  }

  test("Regression with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestRegressor()
    regressionTestWithContinuousFeatures(rf)
  }

  test("Regression with continuous features and node Id cache :" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestRegressor()
      .setCacheNodeIds(true)
    regressionTestWithContinuousFeatures(rf)
  }

  test("Feature importance with toy data") {
    val rf = new RandomForestRegressor()
      .setImpurity("variance")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setFeatureSubsetStrategy("all")
      .setSubsamplingRate(1.0)
      .setSeed(123)

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, 0)

    val model = rf.fit(df)

    val importances = model.featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
      model: RandomForestRegressionModel,
      model2: RandomForestRegressionModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
    }

    val rf = new RandomForestRegressor().setNumTrees(2)
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("impurity" -> "variance")

    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 0)
    testEstimatorAndModelReadWrite(rf, continuousData, allParamSettings, checkModelData)
  }
}

private object RandomForestRegressorSuite extends SparkFunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
    data: RDD[LabeledPoint],
    rf: RandomForestRegressor,
    categoricalFeatures: Map[Int, Int]): Unit = {
    val numFeatures = data.first().features.size
    val oldStrategy =
      rf.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, rf.getOldImpurity)
    val oldModel = OldRandomForest.trainRegressor(data, oldStrategy,
      rf.getNumTrees, rf.getFeatureSubsetStrategy, rf.getSeed.toInt)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 0)
    val newModel = rf.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = RandomForestRegressionModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[RandomForestRegressor], categoricalFeatures)
    TreeTests.checkEqual(oldModelAsNew, newModel)
    assert(newModel.numFeatures === numFeatures)
  }
}
