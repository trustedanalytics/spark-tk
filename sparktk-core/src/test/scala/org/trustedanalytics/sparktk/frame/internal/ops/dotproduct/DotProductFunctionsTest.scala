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
package org.trustedanalytics.sparktk.frame.internal.ops.dotproduct

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.ops.DotProduct
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.VectorFunctions
import org.trustedanalytics.sparktk.testutils.{ MatcherUtils, TestingSparkContextWordSpec }
import MatcherUtils._

class DotProductTest extends TestingSparkContextWordSpec with Matchers {
  val epsilon = 0.000000001

  val inputRows: Array[Row] = Array(
    new GenericRow(Array[Any](1d, 0.2d, -2, 5, Vector(1d, 2d).asInstanceOf[Any], Vector(3d, 4d).asInstanceOf[Any])),
    new GenericRow(Array[Any](2d, 0.4d, -1, 6, Vector(5d, 6d).asInstanceOf[Any], Vector(7d, 8d).asInstanceOf[Any])),
    new GenericRow(Array[Any](3d, 0.6d, 0, 7, Vector(9d, 10d).asInstanceOf[Any], Vector(11d, 12d).asInstanceOf[Any])),
    new GenericRow(Array[Any](4d, 0.8d, 1, 8, Vector(-2d, 3d).asInstanceOf[Any], Vector(-4d, -5d).asInstanceOf[Any])),
    new GenericRow(Array[Any](5d, Double.NaN, 2, Double.NaN, Vector(6d, Double.NaN).asInstanceOf[Any], Vector(Double.NaN, 7d).asInstanceOf[Any])),
    new GenericRow(Array[Any](null, null, null, null, null, null))
  )

  val inputSchema = FrameSchema(Vector(
    Column("col_0", DataTypes.float64),
    Column("col_1", DataTypes.float64),
    Column("col_2", DataTypes.int32),
    Column("col_3", DataTypes.int32),
    Column("col_4", DataTypes.vector(2)),
    Column("col_5", DataTypes.vector(2))
  ))

  "dotProduct" should {
    "compute the dot product for sequences of columns" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)

      val results = DotProduct.dotProduct(frameRdd, List("col_0", "col_1"), List("col_2", "col_3")).collect()
      val dotProducts = results.map(row => row(6).asInstanceOf[Double])

      results.size should be(6)
      dotProducts should equalWithTolerance(Array(-1d, 0.4d, 4.2d, 10.4d, 10d, 0d), epsilon)
    }

    "compute the dot product for sequences of columns using defaults for Double.NaN" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)

      val results = DotProduct.dotProduct(frameRdd, List("col_0", "col_1"), List("col_2", "col_3"),
        Some(List(0.1, 0.2)), Some(List(0.3, 0.4))).collect()
      val dotProducts = results.map(row => row(6).asInstanceOf[Double])

      results.size should be(6)
      dotProducts should equalWithTolerance(Array(-1d, 0.4d, 4.2d, 10.4d, 10.08d, 0.11d), epsilon)
    }

    "compute the dot product for vectors of doubles" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)

      val results = DotProduct.dotProduct(frameRdd, List("col_4"), List("col_5")).collect()
      val dotProducts = results.map(row => row(6).asInstanceOf[Double])

      results.size should be(6)
      dotProducts should equalWithTolerance(Array(11d, 83d, 219d, -7d, 0d, 0d), epsilon)
    }

    "compute the dot product for vectors of doubles using defaults for Double.NaN" in {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)

      val results = DotProduct.dotProduct(frameRdd, List("col_4"), List("col_5"),
        Some(List(0.1, 0.2)), Some(List(0.3, 0.4))).collect()
      val dotProducts = results.map(row => row(6).asInstanceOf[Double])

      results.size should be(6)
      dotProducts should equalWithTolerance(Array(11d, 83d, 219d, -7d, 3.2d, 0.11d), epsilon)
    }
  }

  "computeDotProduct" should {
    "compute the dot product" in {
      val leftVector = Vector(1d, 2d, 3d)
      val rightVector = Vector(4d, 5d, 6d)
      val dotProduct = VectorFunctions.dotProduct(leftVector, rightVector)
      dotProduct should be(32d)
    }

    "throw an IllegalArgumentException if left vector is empty" in {
      intercept[IllegalArgumentException] {
        VectorFunctions.dotProduct(Vector.empty[Double], Vector(1d, 2d, 3d))
      }
    }
    "throw an IllegalArgumentException if right vector is empty" in {
      intercept[IllegalArgumentException] {
        VectorFunctions.dotProduct(Vector(1d, 2d, 3d), Vector.empty[Double])
      }
    }

    "throw an IllegalArgumentException if vectors are not the same size" in {
      intercept[IllegalArgumentException] {
        VectorFunctions.dotProduct(Vector(1d, 2d, 3d), Vector(1d, 2d, 3d, 4d))
      }
    }
  }
}
