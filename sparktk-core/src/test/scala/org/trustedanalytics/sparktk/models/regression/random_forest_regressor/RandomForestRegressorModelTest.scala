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
import org.trustedanalytics.sparktk.models.classification.random_forest_classifier.RandomForestClassifierModel
import org.trustedanalytics.sparktk.models.regression.RegressionTestMetrics
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class RandomForestRegressorModelTest extends TestingSparkContextWordSpec with Matchers {

  val labeledPoint: Array[Row] = Array(
    new GenericRow(Array[Any](1, 16.8973559126, 2.6933495054)),
    new GenericRow(Array[Any](1, 5.5548729596, 2.7777687995)),
    new GenericRow(Array[Any](0, 46.1810010826, 3.1611961917)),
    new GenericRow(Array[Any](0, 44.3117586448, 3.3458963222)))
  val schema = new FrameSchema(List(Column("label", DataTypes.int32), Column("obs1", DataTypes.float64), Column("obs2", DataTypes.float64)))

  "RandomForestRegressorModel" should {
    "create a RandomForestRegressorModel" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, "label", List("obs1", "obs2"), 1, "variance", 4, 100, 10, None, None)
      val featureImp = model.featureImportances()

      model shouldBe a[RandomForestRegressorModel]
      assert(featureImp.size == 2)
      assert(featureImp("obs1") > featureImp("obs2"))
    }

    "create a RandomForestRegressorModel with some categorical features" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](1, 19.8446136104, 0)),
        new GenericRow(Array[Any](1, 16.8973559126, 0)),
        new GenericRow(Array[Any](0, 44.3117586448, 1)),
        new GenericRow(Array[Any](0, 34.6334526911, 1)))
      val frameSchema = new FrameSchema(List(Column("label", DataTypes.int32), Column("continuous_obs", DataTypes.float64),
        Column("categorical_obs", DataTypes.int32)))

      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, frameSchema)
      val featureCategories = Map("categorical_obs" -> 2)
      val model = RandomForestRegressorModel.train(frame, "label", List("continuous_obs", "categorical_obs"),
        1, "variance", 4, 100, 10, Some(featureCategories), None)
      val featureImp = model.featureImportances()

      model shouldBe a[RandomForestRegressorModel]
      assert(featureImp.size == 2)
      assert(featureImp("continuous_obs") > featureImp("categorical_obs"))
    }

    "throw an IllegalArgumentException for empty observationColumns" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)
        val model = RandomForestRegressorModel.train(frame, "label", List(), 1, "variance", 4, 100, 10, None, None)
      }
    }

    "throw an IllegalArgumentException for empty labelColumn" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)
        val model = RandomForestRegressorModel.train(frame, "", List("obs1", "obs2"), 1, "variance", 4, 100, 10, None, None)
      }
    }

    "return predictions when calling score method" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestClassifierModel.train(frame, "label", List("obs1", "obs2"), 2, 1, "gini", 4, 100, 10, None, None)

      // Test data for scoring
      val inputArray = Array[Any](16.8973559126, 2.6933495054)
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

    "test should return a regression metric" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, "label", List("obs1", "obs2"), 1, "variance", 4, 100, 10, None, None)
      val metrics = model.test(frame, "label", None)

      metrics shouldBe a[RegressionTestMetrics]
      metrics.r2 should equal(1.0)
      metrics.meanSquaredError should equal(0)
    }

    "throw IllegalArgumentExceptions for invalid scoring parameters" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestRegressorModel.train(frame, "label", List("obs1", "obs2"), 1, "variance", 4, 100, 10, None, None)

      intercept[IllegalArgumentException] {
        model.score(null)
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any]())
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any](16.8973559126, 2.6933495054, "bogus"))
      }
    }
  }

}
