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
package org.trustedanalytics.sparktk.frame.internal.ops.join

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class JoinCrossTest extends TestingSparkContextWordSpec with Matchers {

  val data1: List[Row] = List(
    new GenericRow(Array[Any](1)),
    new GenericRow(Array[Any](2)),
    new GenericRow(Array[Any](3)))

  val data2: List[Row] = List(
    new GenericRow(Array[Any]("a")),
    new GenericRow(Array[Any]("b")),
    new GenericRow(Array[Any]("c"))
  )

  val data3: List[Row] = List(
    new GenericRow(Array[Any](1)),
    new GenericRow(Array[Any](null)))

  val schema1 = FrameSchema(Vector(
    Column("num", DataTypes.int32)
  ))

  val schema2 = FrameSchema(Vector(
    Column("letters", DataTypes.str)
  ))

  "joinCross" should {
    "return the cartesian product of two frames" in {
      val frame1 = new FrameRdd(schema1, sparkContext.parallelize(data1))
      val frame2 = new FrameRdd(schema2, sparkContext.parallelize(data2))

      val resultFrame = JoinRddFunctions.crossJoin(frame1, frame2)
      val results = resultFrame.collect()

      resultFrame.schema.columns should equal(Vector(
        Column("num", DataTypes.int32),
        Column("letters", DataTypes.string)
      ))

      val expectedResults = Array(
        new GenericRow(Array[Any](1, "a")),
        new GenericRow(Array[Any](1, "b")),
        new GenericRow(Array[Any](1, "c")),
        new GenericRow(Array[Any](2, "a")),
        new GenericRow(Array[Any](2, "b")),
        new GenericRow(Array[Any](2, "c")),
        new GenericRow(Array[Any](3, "a")),
        new GenericRow(Array[Any](3, "b")),
        new GenericRow(Array[Any](3, "c"))
      )

      results should contain theSameElementsAs expectedResults
    }

    "return the cartesian product when cross joining a frame with itself" in {
      val frame = new FrameRdd(schema1, sparkContext.parallelize(data1))

      val resultFrame = JoinRddFunctions.crossJoin(frame, frame)
      val results = resultFrame.collect()

      // Note that column names should be unique, so the second column should be num_R
      resultFrame.schema.columns should equal(Vector(
        Column("num", DataTypes.int32),
        Column("num_R", DataTypes.int32)
      ))

      val expectedResults = Array(
        new GenericRow(Array[Any](1, 1)),
        new GenericRow(Array[Any](1, 2)),
        new GenericRow(Array[Any](1, 3)),
        new GenericRow(Array[Any](2, 1)),
        new GenericRow(Array[Any](2, 2)),
        new GenericRow(Array[Any](2, 3)),
        new GenericRow(Array[Any](3, 1)),
        new GenericRow(Array[Any](3, 2)),
        new GenericRow(Array[Any](3, 3))
      )

      results should contain theSameElementsAs expectedResults
    }

    "return the cartesian product including frame with missing values" in {
      val frame1 = new FrameRdd(schema1, sparkContext.parallelize(data3))
      val frame2 = new FrameRdd(schema2, sparkContext.parallelize(data2))

      val resultFrame = JoinRddFunctions.crossJoin(frame1, frame2)
      val results = resultFrame.collect()

      resultFrame.schema.columns should equal(Vector(
        Column("num", DataTypes.int32),
        Column("letters", DataTypes.string)
      ))

      val expectedResults = Array(
        new GenericRow(Array[Any](1, "a")),
        new GenericRow(Array[Any](1, "b")),
        new GenericRow(Array[Any](1, "c")),
        new GenericRow(Array[Any](null, "a")),
        new GenericRow(Array[Any](null, "b")),
        new GenericRow(Array[Any](null, "c"))
      )

      results should contain theSameElementsAs expectedResults
    }
  }
}