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
package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column, FrameSchema, Frame }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ReverseBoxCoxTest extends TestingSparkContextWordSpec with Matchers {

  val reverseBoxCoxRows: Array[Row] = Array(new GenericRow(Array[Any](2.8191327990706947, 1.2)),
    new GenericRow(Array[Any](-1.253653813751204, 2.3)),
    new GenericRow(Array[Any](2.4667363875200685, 3.4)),
    new GenericRow(Array[Any](2.7646912600285254, 4.5)),
    new GenericRow(Array[Any](2.064011015565803, 5.6)))

  val reverseBoxCoxSchema = FrameSchema(List(Column("A", DataTypes.float64), Column("B", DataTypes.float64)))

  "reverseBoxCox" should {
    "compute the reverse box-cox transformation for the given column" in {

      val rdd = sparkContext.parallelize(reverseBoxCoxRows)
      val frameRdd = new FrameRdd(reverseBoxCoxSchema, rdd)

      val results = ReverseBoxCox.reverseBoxCox(frameRdd, "A", 0.3)
      val reverseBoxCox = results.map(row => row(2).asInstanceOf[Double]).collect()

      assert(reverseBoxCox sameElements Array(7.713206432669999, 0.20751949359399996, 6.3364823492600015, 7.488038825390002, 4.98507012303))
    }
  }

  "reverseBoxCox" should {
    "create a new column in the frame storing the reverse box-cox computation with lambda 0.3" in {

      val rdd = sparkContext.parallelize(reverseBoxCoxRows)
      val frameRdd = new FrameRdd(reverseBoxCoxSchema, rdd)

      val result = ReverseBoxCox.reverseBoxCox(frameRdd, "A", 0.3).collect()

      assert(result.length == 5)
      assert(result.apply(0) == Row(2.8191327990706947, 1.2, 7.713206432669999))
      assert(result.apply(1) == Row(-1.253653813751204, 2.3, 0.20751949359399996))
      assert(result.apply(2) == Row(2.4667363875200685, 3.4, 6.3364823492600015))
      assert(result.apply(3) == Row(2.7646912600285254, 4.5, 7.488038825390002))
      assert(result.apply(4) == Row(2.064011015565803, 5.6, 4.98507012303))
    }
  }

  "computeReverseBoxCox" should {
    "compute reverse BoxCox transformation with lambda 0.0" in {

      val input = 1.60644747012
      val lambda = 0.0
      val reverseBoxCoxTransform = ReverseBoxCox.computeReverseBoxCoxTransformation(input, lambda)

      assert(reverseBoxCoxTransform == 4.985070123023598)
    }
  }
}