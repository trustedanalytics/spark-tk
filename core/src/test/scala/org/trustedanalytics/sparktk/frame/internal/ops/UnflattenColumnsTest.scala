/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.ops.unflatten.UnflattenColumnsFunctions
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.spark.sql.Row
import org.scalatest.{ BeforeAndAfterEach, WordSpec, Matchers }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class UnflattenColumnTest extends WordSpec with Matchers with BeforeAndAfterEach with TestingSparkContextWordSpec {
  private val nameColumn = "name"
  private val dateColumn = "date"

  val dailyHeartbeats_4_1 = List(
    Array[Any]("Bob", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "2", "70"),
    Array[Any]("Bob", "1/1/2015", "3", "65"),
    Array[Any]("Bob", "1/1/2015", "4", "55"))

  val dailyHeartbeats_1_1 = List(
    Array[Any]("Bob", "1/1/2015", "1", "60"))

  val dailyHeartbeats_2_2 = List(
    Array[Any]("Mary", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "1", "60"))

  val dailyHeartbeats_4_3 = List(
    Array[Any]("Mary", "1/1/2015", "1", "60"),
    Array[Any]("Bob", "1/1/2015", "1", "60"),
    Array[Any]("Mary", "1/1/2015", "2", "55"),
    Array[Any]("Sue", "1/1/2015", "1", "65")
  )

  def executeTest(data: List[Array[Any]], rowsInResult: Int): Array[Row] = {
    val schema = FrameSchema(Vector(Column(nameColumn, DataTypes.string),
      Column(dateColumn, DataTypes.string),
      Column("minute", DataTypes.int32),
      Column("heartRate", DataTypes.int32)))
    val compositeKeyColumnNames = List(nameColumn, dateColumn)
    val compositeKeyIndices = List(0, 1)

    val rows = sparkContext.parallelize(data)
    val rdd = FrameRdd.toFrameRdd(schema, rows).groupByRows(row => row.values(compositeKeyColumnNames.toVector))

    val targetSchema = UnflattenColumnsFunctions.createTargetSchema(schema, compositeKeyColumnNames)
    val resultRdd = UnflattenColumnsFunctions.unflattenRddByCompositeKey(compositeKeyIndices, rdd, targetSchema, ",")

    resultRdd.take(rowsInResult)
  }

  "UnflattenRddByCompositeKey" should {
    "compress data in a single row (dailyHeartbeats_4_1)" in {

      val rowInResult = 1
      val result = executeTest(dailyHeartbeats_4_1, rowInResult)

      assert(result.length == rowInResult)
      result.apply(rowInResult - 1) shouldBe Row("Bob", "1/1/2015", "1,2,3,4", "60,70,65,55")
    }

    "compress data in a single row (dailyHeartbeats_1_1)" in {

      val rowInResult = 1
      val result = executeTest(dailyHeartbeats_1_1, rowInResult)

      assert(result.length == rowInResult)
      result.apply(rowInResult - 1) shouldBe Row("Bob", "1/1/2015", "1", "60")
    }

    "compress data in two rows (dailyHeartbeats_2_2)" in {
      val rowInResult = 2
      val result = executeTest(dailyHeartbeats_2_2, rowInResult)

      assert(result.length == rowInResult)
    }

    "compress data from four rows into three rows (dailyHeartbeats_4_3)" in {
      val rowInResult = 3
      val result = executeTest(dailyHeartbeats_4_3, rowInResult)
      assert(result.length == rowInResult)
    }
  }
}
