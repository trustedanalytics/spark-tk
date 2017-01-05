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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.models.regression.RegressionTestMetrics
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class RandomForestRegressorModelTest extends TestingSparkContextWordSpec with Matchers {

  val labeledPoint: Array[Row] = Array(
    new GenericRow(Array[Any](1, 16.89, 2.69)),
    new GenericRow(Array[Any](1, 15.55, 2.77)),
    new GenericRow(Array[Any](0, 46.18, 3.16)),
    new GenericRow(Array[Any](0, 44.31, 3.34)))
  val schema = new FrameSchema(List(Column("label", DataTypes.int32), Column("obs1", DataTypes.float64), Column("obs2", DataTypes.float64)))
  val epsilon = 1e-6

  "RandomForestRegressorModel" should {
    "create a RandomForestRegressorModel" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, List("obs1", "obs2"), "label", 1, "variance", 4, 100, 2, 1.0, seed = 10)
      val featureImp = model.featureImportances()

      model shouldBe a[RandomForestRegressorModel]
      assert(model.observationColumns == List("obs1", "obs2"))
      assert(model.labelColumn == "label")
      assert(model.numTrees == 1)
      assert(model.impurity == "variance")
      assert(model.maxDepth == 4)
      assert(model.maxBins == 100)
      assert(model.minInstancesPerNode == 2)
      assert(Math.abs(model.subSamplingRate - 1.0) <= epsilon)
      assert(model.seed == 10)
      assert(model.featureSubsetCategory == "auto")
      assert(model.categoricalFeaturesInfo.isEmpty)

      assert(featureImp.size == 2)
      assert(featureImp("obs1") > featureImp("obs2"))
    }

    "create a RandomForestRegressorModel with some categorical features" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](1, 19.84, 0)),
        new GenericRow(Array[Any](1, 16.89, 0)),
        new GenericRow(Array[Any](0, 44.31, 1)),
        new GenericRow(Array[Any](0, 34.63, 1)))
      val frameSchema = new FrameSchema(List(Column("label", DataTypes.int32), Column("continuous_obs", DataTypes.float64),
        Column("categorical_obs", DataTypes.int32)))

      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, frameSchema)
      val categoricalFeatures = Map("categorical_obs" -> 2)
      val model = RandomForestRegressorModel.train(frame, List("continuous_obs", "categorical_obs"), "label",
        1, "variance", 4, 100, seed = 10, categoricalFeaturesInfo = Some(categoricalFeatures))
      val featureImp = model.featureImportances()

      model shouldBe a[RandomForestRegressorModel]
      assert(model.observationColumns == List("continuous_obs", "categorical_obs"))
      assert(model.labelColumn == "label")
      assert(model.numTrees == 1)
      assert(model.impurity == "variance")
      assert(model.maxDepth == 4)
      assert(model.maxBins == 100)
      assert(model.minInstancesPerNode == 1)
      assert(Math.abs(model.subSamplingRate - 1.0) <= epsilon)
      assert(model.seed == 10)
      assert(model.featureSubsetCategory == "auto")
      assert(model.categoricalFeaturesInfo.isDefined)
      assert(model.categoricalFeaturesInfo.get.size == 1)
      assert(model.categoricalFeaturesInfo.get("categorical_obs") == 2)

      assert(featureImp.size == 2)
      assert(featureImp("continuous_obs") > featureImp("categorical_obs"))
    }

    "throw an IllegalArgumentException for empty observationColumns" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)
        val model = RandomForestRegressorModel.train(frame, List(), "label", 1, "variance", 4, 100, seed = 10)
      }
    }

    "throw an IllegalArgumentException for empty labelColumn" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)
        val model = RandomForestRegressorModel.train(frame, List("obs1", "obs2"), "", 1, "variance", 4, 100, seed = 10)
      }
    }

    "return predictions when calling score method" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, List("obs1", "obs2"), "label", 1, "variance", 4, 100, seed = 10)

      // Test data for scoring
      val inputArray = Array[Any](16.89, 2.69)
      val expectedPredict = 1.0

      // Score and check the results
      val scoreResult = model.score(inputArray)
      assert(inputArray.length == model.input.length)
      assert(scoreResult.length == model.output.length)
      assert(scoreResult.slice(0, inputArray.length).sameElements(inputArray))
      scoreResult(inputArray.length) match {
        case prediction: Double => assert(prediction == expectedPredict)
        case _ => throw new RuntimeException(s"Expected prediction to be a Double but is ${scoreResult(1).getClass.getSimpleName}")
      }
    }

    "predict should return predicted values" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)

      val model = RandomForestRegressorModel.train(frame, List("obs1", "obs2"), "label", 1, "variance", 4, 100, seed = 10)
      val predictFrame = model.predict(frame)
      val labelIndex = predictFrame.schema.columnIndex("label")
      val predictIndex = predictFrame.schema.columnIndex("predicted_value")

      predictFrame.collect().foreach(row => {
        assert(row.getInt(labelIndex).toDouble == row.getDouble(predictIndex))
      })
    }

    "test should return a regression metric" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, List("obs1", "obs2"), "label", 1, "variance", 4, 100, seed = 10)
      val metrics = model.test(frame)

      metrics shouldBe a[RegressionTestMetrics]
      metrics.r2 should equal(1.0)
      metrics.meanSquaredError should equal(0)
    }

    "throw IllegalArgumentExceptions for invalid scoring parameters" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, List("obs1", "obs2"), "label", 1, "variance", 4, 100, seed = 10)

      intercept[IllegalArgumentException] {
        model.score(null)
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any]())
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any](16.89, 2.69, "bogus"))
      }
    }
  }

}
