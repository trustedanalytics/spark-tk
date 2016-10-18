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
package org.trustedanalytics.sparktk.models.clustering.gmm

import org.apache.spark.mllib.org.trustedanalytics.sparktk.MllibAliases
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.RowWrapper
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

import org.apache.spark.sql._

class GaussianMixtureModelTest extends TestingSparkContextWordSpec with Matchers {

  "GaussianMixtureModel score" should {

    val schema = FrameSchema(List(
      Column("data", DataTypes.float32),
      Column("name", DataTypes.string)
    ))

    val data: List[Row] = List(
      new GenericRow(Array[Any](2, "ab")),
      new GenericRow(Array[Any](1, "cd")),
      new GenericRow(Array[Any](7, "ef")),
      new GenericRow(Array[Any](1, "gh")),
      new GenericRow(Array[Any](9, "ij")),
      new GenericRow(Array[Any](2, "kl")),
      new GenericRow(Array[Any](0, "mn")),
      new GenericRow(Array[Any](6, "op")),
      new GenericRow(Array[Any](5, "qr"))
    )

    "return a predicted cluster" in {
      val rdd = sparkContext.parallelize(data)
      val frame = new Frame(rdd, schema)

      // train model
      val trainedModel = GaussianMixtureModel.train(frame, List("data"), List(1.0), 3, seed = 1)

      // input array
      val point = 9.0
      val input = Array[Any](point)
      assert(input.length == trainedModel.input.length)

      // score
      val result = trainedModel.score(input)
      assert(result.length == trainedModel.output.length)
      assert(result(0) == point)
      result(1) match {
        case cluster: Int =>
        case _ => throw new RuntimeException("Expected Int from gmm scoring")
      }
    }

    "throw IllegalArgumentExceptions for invalid scoring parameters" in {
      val rdd = sparkContext.parallelize(data)
      val frame = new Frame(rdd, schema)
      val trainedModel = GaussianMixtureModel.train(frame, List("data"), List(1.0), 3, seed = 1)

      intercept[IllegalArgumentException] {
        trainedModel.score(null)
      }

      intercept[IllegalArgumentException] {
        trainedModel.score(Array[Any]("invalid"))
      }

      intercept[IllegalArgumentException] {
        trainedModel.score(Array[Any](1.0, 2.0))
      }
    }
  }

}