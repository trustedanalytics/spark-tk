/**
 *  Copyright (c) 2015 Intel Corporation 
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
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

class JoinBroadcastVariableITest extends TestingSparkContextWordSpec with Matchers {
  val idCountryNames: List[Row] = List(
    new GenericRow(Array[Any](1, "Iceland")),
    new GenericRow(Array[Any](1, "Ice-land")),
    new GenericRow(Array[Any](2, "India")),
    new GenericRow(Array[Any](3, "Norway")),
    new GenericRow(Array[Any](4, "Oman")),
    new GenericRow(Array[Any](6, "Germany"))
  )

  val inputSchema = FrameSchema(Vector(
    Column("col_0", DataTypes.int32),
    Column("col_1", DataTypes.str)
  ))

  "JoinBroadcastVariable" should {
    "create a single broadcast variable" in {
      val countryNames = new FrameRdd(inputSchema, sparkContext.parallelize(idCountryNames))

      val joinParam = RddJoinParam(countryNames, Seq("col_0"))

      val broadcastVariable = JoinBroadcastVariable(joinParam)

      broadcastVariable.broadcastMultiMap.value.size should equal(5)
      broadcastVariable.get(List(1)).get should contain theSameElementsAs List(idCountryNames(0), idCountryNames(1))
      broadcastVariable.get(List(2)).get should contain theSameElementsAs List(idCountryNames(2))
      broadcastVariable.get(List(3)).get should contain theSameElementsAs List(idCountryNames(3))
      broadcastVariable.get(List(4)).get should contain theSameElementsAs List(idCountryNames(4))
      broadcastVariable.get(List(6)).get should contain theSameElementsAs List(idCountryNames(5))
      broadcastVariable.get(List(8)).isDefined should equal(false)

    }

    "create an empty broadcast variable" in {
      val countryNames = new FrameRdd(inputSchema, sparkContext.parallelize(List.empty[Row]))

      val joinParam = RddJoinParam(countryNames, Seq("col_0"))

      val broadcastVariable = JoinBroadcastVariable(joinParam)

      broadcastVariable.broadcastMultiMap.value.isEmpty should equal(true)
    }

    "throw an IllegalArgumentException if join parameter is null" in {
      intercept[IllegalArgumentException] {
        JoinBroadcastVariable(null)
      }
    }
    "throw an Exception if column does not exist in frame" in {
      intercept[Exception] {
        val countryNames = new FrameRdd(inputSchema, sparkContext.parallelize(idCountryNames))
        val joinParam = RddJoinParam(countryNames, Seq("col_bad"))
        JoinBroadcastVariable(joinParam)
      }
    }

  }
}
