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
package org.trustedanalytics.sparktk.models.regression.linear_regression

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.models.regression.RegressionTestMetrics
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class LinearRegressionTest extends TestingSparkContextWordSpec with Matchers {

  val labeledPoint: Array[Row] = Array(
    new GenericRow(Array[Any](1.0, 1.0)),
    new GenericRow(Array[Any](2.0, 2.0)),
    new GenericRow(Array[Any](3.0, 3.0)),
    new GenericRow(Array[Any](4.0, 4.0)))
  val schema = new FrameSchema(List(Column("label", DataTypes.float64), Column("obs1", DataTypes.float64)))

  "Linear Regression" should {
    "create a linear regression model" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "label", List("obs1"))

      model shouldBe a[LinearRegressionModel]
      model.valueColumn should equal("label")
      model.observationColumnsTrain should equal(List("obs1"))
      model.intercept should equal(0.0)
      model.weights should equal(List(1.0))
      model.r2 should equal(1.0)
      model.iterations should equal(1)
    }

    "test should return a regression metric" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "label", List("obs1"))
      val metrics = model.test(frame, "label", None)

      metrics shouldBe a[RegressionTestMetrics]
      metrics.r2 should equal(1.0)
      metrics.explainedVarianceScore should equal(1.0)
      metrics.meanSquaredError should equal(0)
    }

    "predict the results of linear regression correctly" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "label", List("obs1"))

      val predictFrame = model.predict(frame, None)
      predictFrame.schema.columns should equal(List(Column("label", DataTypes.float64), Column("obs1", DataTypes.float64), Column("predicted_value", DataTypes.float64)))
      predictFrame.rdd.toArray.toList should equal(List(
        new GenericRow(Array[Any](1.0, 1.0, 1.0)),
        new GenericRow(Array[Any](2.0, 2.0, 2.0)),
        new GenericRow(Array[Any](3.0, 3.0, 3.0)),
        new GenericRow(Array[Any](4.0, 4.0, 4.0))))
    }
    "throw an exception for null frame" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val thrown = the[IllegalArgumentException] thrownBy LinearRegressionModel.train(null, "label", List("obs1"))
      thrown.getMessage should equal("requirement failed: frame is required")
    }

    "throw an exception for null frame in predict" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "label", List("obs1"))

      val thrown = the[IllegalArgumentException] thrownBy model.predict(null, None)
      thrown.getMessage should equal("requirement failed: require frame to predict")
    }

    "throw an exception for invalid test column" in {

      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val model = LinearRegressionModel.train(frame, "label", List("obs1"))

      val thrown = the[IllegalArgumentException] thrownBy model.test(null, "label", Some(List("obs1", "obs2")))
      thrown.getMessage should equal("requirement failed: Number of columns for train and test should be same")
    }

    "throw an exception for null value column" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val thrown = the[IllegalArgumentException] thrownBy LinearRegressionModel.train(frame, null, List("obs1"))
      thrown.getMessage should equal("requirement failed: valueColumn must not be null nor empty")
    }

    "throw an exception for null observation column" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val thrown = the[IllegalArgumentException] thrownBy LinearRegressionModel.train(frame, "label", null)
      thrown.getMessage should equal("requirement failed: observationColumn must not be null nor empty")
    }

    "throw an exception for max iterations < 0" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val thrown = the[IllegalArgumentException] thrownBy LinearRegressionModel.train(frame, "label", List("obs1"), maxIterations = (-1))
      thrown.getMessage should equal("requirement failed: numIterations must be a positive value")
    }

    "throw an exception for reg param < 0" in {
      val rdd = sparkContext.parallelize(labeledPoint)
      val frame = new Frame(rdd, schema)
      val thrown = the[IllegalArgumentException] thrownBy LinearRegressionModel.train(frame, "label", List("obs1"), regParam = (-1.0))
      thrown.getMessage should equal("requirement failed: regParam should be greater than or equal to 0")
    }

  }

}
