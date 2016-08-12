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

package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class AppendTest extends TestingSparkContextWordSpec with Matchers {

  "frame append" should {
    val rowsA: Array[Row] = Array(
      new GenericRow(Array[Any](1)),
      new GenericRow(Array[Any](2)),
      new GenericRow(Array[Any](3))
    )

    val rowsB: Array[Row] = Array(
      new GenericRow(Array[Any](4)),
      new GenericRow(Array[Any](5)),
      new GenericRow(Array[Any](6))
    )

    val schema = FrameSchema(Vector(Column("integer", DataTypes.int32)))

    "append the contents of the frame, when the schemas match" in {
      val rddA = sparkContext.parallelize(rowsA)
      val rddB = sparkContext.parallelize(rowsB)
      val frameA = new Frame(rddA, schema)
      val frameB = new Frame(rddB, schema)
      assert(frameA.rowCount == 3)
      assert(frameB.rowCount == 3)

      // Append frameB to frameA
      frameA.append(frameB)
      assert(frameA.rowCount == 6)
      val frameRows = frameA.rdd.take(frameA.rowCount.toInt)
      frameRows.foreach((r: Row) => assert(r.length == 1))
      val values = frameRows.map((r: Row) => r.getInt(0)).toList
      assert(values.equals(List(1, 2, 3, 4, 5, 6)))
    }

    "append a frame that has more columns" in {
      val rddA = sparkContext.parallelize(rowsA)
      val frameA = new Frame(rddA, schema)

      val strRows: Array[Row] = Array(
        new GenericRow(Array[Any](4, "4")),
        new GenericRow(Array[Any](5, "5")),
        new GenericRow(Array[Any](6, "6"))
      )
      val rddB = sparkContext.parallelize(strRows)
      val schemaB = FrameSchema(Vector(Column("integer", DataTypes.int32), Column("num", DataTypes.string)))
      val frameB = new Frame(rddB, schemaB)

      // Append frameB to frameA.  Since frameB has an extra column that should get added to the schema.
      frameA.append(frameB)
      assert(frameA.rowCount() == 6)
      assert(frameA.schema == schemaB)
      val values = frameA.rdd.take(frameA.rowCount.toInt).map((r: Row) => (r.getInt(0), r.getString(1))).toList
      assert(values.equals(List((1, null), (2, null), (3, null), (4, "4"), (5, "5"), (6, "6"))))
    }

    "append using an empty frame" in {
      val emptyRdd = sparkContext.emptyRDD[Row]
      val emptyFrame = new Frame(emptyRdd, schema)

      val rddA = sparkContext.parallelize(rowsA)
      val frameA = new Frame(rddA, schema)

      assert(emptyFrame.rowCount == 0)
      assert(frameA.rowCount == 3)

      // append the empty frame to frameA - it should still have 3 rows and schema should be unchanged
      frameA.append(emptyFrame)
      assert(frameA.rowCount == 3)
      assert(frameA.schema == schema)

      // append frameA to the empty frame
      emptyFrame.append(frameA)
      assert(emptyFrame.rowCount() == 3)
      assert(emptyFrame.schema == schema)
      val values = emptyFrame.rdd.take(emptyFrame.rowCount.toInt).map((r: Row) => r.getInt(0)).toList
      assert(values.equals(List(1, 2, 3)))
    }
  }
}