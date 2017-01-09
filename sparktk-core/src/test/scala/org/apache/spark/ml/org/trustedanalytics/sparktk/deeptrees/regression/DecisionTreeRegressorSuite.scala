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
import org.apache.spark.ml.org.trustedanalytics.sparktk.deeptrees.util._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.org.trustedanalytics.sparktk.deeptrees.tree.{ DecisionTree => OldDecisionTree, DecisionTreeSuite => OldDecisionTreeSuite }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }
import TestingUtils._

class DecisionTreeRegressorSuite
    extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import DecisionTreeRegressorSuite.compareAPIs

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPoints())
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  test("Regression stump with 3-ary (ordered) categorical features") {
    val dt = new DecisionTreeRegressor()
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(100)
      .setSeed(1)
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures)
  }

  test("Regression stump with binary (ordered) categorical features") {
    val dt = new DecisionTreeRegressor()
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 2, 1 -> 2)
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures)
  }

  test("copied model must have the same parent") {
    val categoricalFeatures = Map(0 -> 2, 1 -> 2)
    val df = TreeTests.setMetadata(categoricalDataPointsRDD, categoricalFeatures, numClasses = 0)
    val model = new DecisionTreeRegressor()
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(8).fit(df)
    MLTestingUtils.checkCopy(model)
  }

  test("predictVariance") {
    val dt = new DecisionTreeRegressor()
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(100)
      .setPredictionCol("")
      .setVarianceCol("variance")
    val categoricalFeatures = Map(0 -> 2, 1 -> 2)

    val df = TreeTests.setMetadata(categoricalDataPointsRDD, categoricalFeatures, numClasses = 0)
    val model = dt.fit(df)

    val predictions = model.transform(df)
      .select(model.getFeaturesCol, model.getVarianceCol)
      .collect()

    predictions.foreach {
      case Row(features: Vector, variance: Double) =>
        val expectedVariance = model.rootNode.predictImpl(features).impurityStats.calculate()
        assert(variance === expectedVariance,
          s"Expected variance $expectedVariance but got $variance.")
    }

    val varianceData: RDD[LabeledPoint] = TreeTests.varianceData(sc)
    val varianceDF = TreeTests.setMetadata(varianceData, Map.empty[Int, Int], 0)
    dt.setMaxDepth(1)
      .setMaxBins(6)
      .setSeed(0)
    val transformVarDF = dt.fit(varianceDF).transform(varianceDF)
    val calculatedVariances = transformVarDF.select(dt.getVarianceCol).collect().map {
      case Row(variance: Double) => variance
    }

    // Since max depth is set to 1, the best split point is that which splits the data
    // into (0.0, 1.0, 2.0) and (10.0, 12.0, 14.0). The predicted variance for each
    // data point in the left node is 0.667 and for each data point in the right node
    // is 2.667
    val expectedVariances = Array(0.667, 0.667, 0.667, 2.667, 2.667, 2.667)
    calculatedVariances.zip(expectedVariances).foreach {
      case (actual, expected) =>
        assert(actual ~== expected absTol 1e-3)
    }
  }

  test("Feature importance with toy data") {
    val dt = new DecisionTreeRegressor()
      .setImpurity("variance")
      .setMaxDepth(3)
      .setSeed(123)

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, 0)

    val model = dt.fit(df)

    val importances = model.featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  test("should support all NumericType labels and not support other types") {
    val dt = new DecisionTreeRegressor().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[DecisionTreeRegressionModel, DecisionTreeRegressor](
      dt, sqlContext, isClassification = false) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
      model: DecisionTreeRegressionModel,
      model2: DecisionTreeRegressionModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
    }

    val dt = new DecisionTreeRegressor()
    val rdd = TreeTests.getTreeReadWriteData(sc)

    // Categorical splits with tree depth 2
    val categoricalData: DataFrame =
      TreeTests.setMetadata(rdd, Map(0 -> 2, 1 -> 3), numClasses = 0)
    testEstimatorAndModelReadWrite(dt, categoricalData,
      TreeTests.allParamSettings, checkModelData)

    // Continuous splits with tree depth 2
    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 0)
    testEstimatorAndModelReadWrite(dt, continuousData,
      TreeTests.allParamSettings, checkModelData)

    // Continuous splits with tree depth 0
    testEstimatorAndModelReadWrite(dt, continuousData,
      TreeTests.allParamSettings ++ Map("maxDepth" -> 0), checkModelData)
  }
}

private[ml] object DecisionTreeRegressorSuite extends SparkFunSuite {

  /**
   * Train 2 decision trees on the given dataset, one using the old API and one using the new API.
   * Convert the old tree to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
    data: RDD[LabeledPoint],
    dt: DecisionTreeRegressor,
    categoricalFeatures: Map[Int, Int]): Unit = {
    val numFeatures = data.first().features.size
    val oldStrategy = dt.getOldStrategy(categoricalFeatures)
    val oldTree = OldDecisionTree.train(data, oldStrategy)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 0)
    val newTree = dt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldTreeAsNew = DecisionTreeRegressionModel.fromOld(
      oldTree, newTree.parent.asInstanceOf[DecisionTreeRegressor], categoricalFeatures)
    TreeTests.checkEqual(oldTreeAsNew, newTree)
    assert(newTree.numFeatures === numFeatures)
  }
}
