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
    }

  }

}
