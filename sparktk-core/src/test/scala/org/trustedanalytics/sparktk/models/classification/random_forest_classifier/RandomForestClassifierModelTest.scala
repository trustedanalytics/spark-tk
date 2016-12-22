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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.ops.classificationmetrics.{ ClassificationMetricValue, BinaryClassificationMetrics }
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class RandomForestClassifierModelTest extends TestingSparkContextWordSpec with Matchers {

  val labeledPoint: Array[Row] = Array(
    new GenericRow(Array[Any](1, 16.8973559126, 2.6933495054)),
    new GenericRow(Array[Any](1, 5.5548729596, 2.7777687995)),
    new GenericRow(Array[Any](0, 46.1810010826, 3.1611961917)),
    new GenericRow(Array[Any](0, 44.3117586448, 3.3458963222)))
  val schema = new FrameSchema(List(Column("label", DataTypes.int32), Column("obs1", DataTypes.float64), Column("obs2", DataTypes.float64)))

  "RandomForestClassifierModel" should {
    "create a RandomForestClassifierModel" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)

      val model = RandomForestClassifierModel.train(frame, List("obs1", "obs2"), "label", 2, 1, "gini", 4, 100, 10, None, None)
      val featureImp = model.featureImportances()

      model shouldBe a[RandomForestClassifierModel]
      assert(featureImp.size == 2)
      assert(featureImp("obs1") > featureImp("obs2"))
    }

    "create a RandomForestClassifierModel with some categorical features" in {
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
      val model = RandomForestClassifierModel.train(frame, List("continuous_obs", "categorical_obs"), "label", 2,
        1, "gini", 4, 100, 10, Some(featureCategories), None)
      val featureImp = model.featureImportances()

      model shouldBe a[RandomForestClassifierModel]
      assert(featureImp.size == 2)
      assert(featureImp("continuous_obs") > featureImp("categorical_obs"))
    }

    "predict should return predicted classes" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)

      val model = RandomForestClassifierModel.train(frame, List("obs1", "obs2"), "label", 2, 1, "gini", 4, 100, 10, None, None)
      val predictFrame = model.predict(frame)
      val labelIndex = predictFrame.schema.columnIndex("label")
      val predictIndex = predictFrame.schema.columnIndex("predicted_class")

      predictFrame.collect().foreach(row => {
        assert(row.getInt(labelIndex) == row.getInt(predictIndex))
      })
    }

    "test should return a classification metric" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)

      val model = RandomForestClassifierModel.train(frame, List("obs1", "obs2"), "label", 2, 1, "gini", 4, 100, 10, None, None)
      val metrics = model.test(frame)
      val confusionMatrix = metrics.confusionMatrix

      metrics shouldBe a[ClassificationMetricValue]
      assert(metrics.accuracy == 1.0)
      assert(metrics.fMeasure == 1.0)
      assert(metrics.precision == 1.0)
      assert(metrics.recall == 1.0)
      assert(confusionMatrix.numRows == 2)
      assert(confusionMatrix.numColumns == 2)
      assert(confusionMatrix.get("pos", "pos") == 2)
      assert(confusionMatrix.get("pos", "neg") == 0)
      assert(confusionMatrix.get("neg", "pos") == 0)
      assert(confusionMatrix.get("neg", "neg") == 2)
    }

    "throw an IllegalArgumentException for empty observationColumns" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = RandomForestClassifierModel.train(frame, List(), "label", 2, 1, "gini", 4, 100, 10, None, None)
      }
    }

    "throw an IllegalArgumentException for empty labelColumn" in {
      intercept[IllegalArgumentException] {

        val rdd = sparkContext.parallelize(labeledPoint)
        val frame = new Frame(rdd, schema)

        val model = RandomForestClassifierModel.train(frame, List("obs1", "obs2"), "", 2, 1, "gini", 4, 100, 10, None, None)
      }
    }

    "return predictions when calling the random forest classifier model score" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestClassifierModel.train(frame, List("obs1", "obs2"), "label", 2, 1, "gini", 4, 100, 10, None, None)

      // Test values for scoring
      val inputValues = Array[Any](44.3117586448, 3.3458963222)
      val classLabel = 0.0

      // Score and check the results
      val scoreResult = model.score(inputValues)
      assert(inputValues.length == model.input.length)
      assert(scoreResult.length == model.output.length)
      assert(scoreResult.slice(0, inputValues.length).sameElements(inputValues))
      scoreResult(inputValues.length) match {
        case prediction: Double => assert(prediction == classLabel)
        case _ => throw new RuntimeException(s"Expected prediction to be a Double but is ${scoreResult(1).getClass.getSimpleName}")
      }
    }

    "throw IllegalArgumentExceptions for invalid scoring parameters" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = RandomForestClassifierModel.train(frame, List("obs1", "obs2"), "label", 2, 1, "gini", 4, 100, 10, None, None)

      intercept[IllegalArgumentException] {
        model.score(null)
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any]())
      }

      intercept[IllegalArgumentException] {
        model.score(Array[Any](44.3117586448, 3.3458963222, "bogus"))
      }
    }
  }

}
