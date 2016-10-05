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
import org.trustedanalytics.sparktk.frame.{ Frame, DataTypes, Column, FrameSchema }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class CopyTest extends TestingSparkContextWordSpec with Matchers {
  "Copy" should {
    val rows: Array[Row] = Array(
      new GenericRow(Array[Any](1, "one")),
      new GenericRow(Array[Any](2, "two")),
      new GenericRow(Array[Any](3, "three")),
      new GenericRow(Array[Any](4, "four")),
      new GenericRow(Array[Any](5, "five"))
    )
    val schema = FrameSchema(Vector(
      Column("number_int", DataTypes.int32),
      Column("number_str", DataTypes.string)
    ))

    "include all rows and all columns by default" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val newFrame = frame.copy()

      // Check to ensure new_frame has the same contents and schema as the original
      assert(newFrame.rdd.count == frame.rdd.count)
      assert(newFrame.schema.columns == frame.schema.columns)
      val newFrameValues = newFrame.take(newFrame.rdd.count.toInt)
      assert(newFrameValues.length == newFrame.rdd.count.toInt)
      newFrameValues.foreach((r: Row) => assert(r.length == 2)) // each row should have 2 columns

      // Ensure that modifying the new_frame does not affect the original frame
      newFrame.binColumn("number_int", None)
      assert(newFrame.schema.columns.length != frame.schema.columns.length)
    }

    "filter columns when dictionary of columns is specified" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)

      // Copy with map that renames 'number_int' to 'integer'
      val columnMap = Map("number_int" -> "integer", "number_str" -> "number_str")
      val newFrame = frame.copy(Some(columnMap))

      // Check row count and columns
      assert(newFrame.rdd.count == frame.rdd.count)
      assert(newFrame.schema.columns.length == frame.schema.columns.length)
      assert(newFrame.schema.hasColumn("number_int") == false)
      assert(newFrame.schema.hasColumn("integer") == true)
      assert(newFrame.schema.hasColumn("number_str") == true)
      val newFrameValues = newFrame.take(newFrame.rdd.count.toInt)
      assert(newFrameValues.length == newFrame.rdd.count.toInt)
      newFrameValues.foreach((r: Row) => assert(r.length == 2)) // each row should only have one column

      // Copy with only one column in the map
      val numberColumnMap = Map("number_int" -> "integer")
      val numberFrame = frame.copy(Some(numberColumnMap))
      assert(numberFrame.rdd.count == frame.rdd.count)
      assert(numberFrame.schema.columns.length == 1)
      assert(numberFrame.schema.hasColumn("number_int") == false)
      assert(numberFrame.schema.hasColumn("integer") == true)
      assert(numberFrame.schema.hasColumn("number_str") == false)
      val numberFrameValues = numberFrame.take(numberFrame.rdd.count.toInt)
      assert(numberFrameValues.length == numberFrame.rdd.count.toInt)
      numberFrameValues.foreach((r: Row) => assert(r.length == 1)) // each row should only have one column
    }

    "filter data when where function is specified" in {
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, schema)
      val columnIndex = schema.columnIndex("number_int")

      def greaterThanTwo(row: Row): Boolean = {
        row.getInt(columnIndex) > 2
      }

      // Copy with no column map
      val newFrame = frame.copy(None, Some(greaterThanTwo))
      assert(newFrame.rdd.count == 3)
      assert(newFrame.schema.columns.length == 2)

      // Copy with column map
      val newFrameStringColumn = frame.copy(Some(Map("number_str" -> "number")), Some(greaterThanTwo))
      assert(newFrameStringColumn.rdd.count == 3)
      assert(newFrameStringColumn.schema.columns.length == 1)
      assert(newFrameStringColumn.schema.hasColumn("number_str") == false)
      assert(newFrameStringColumn.schema.hasColumn("number") == true)
      assert(newFrameStringColumn.schema.hasColumn("number_int") == false)
      val newFrameValues = newFrameStringColumn.take(newFrameStringColumn.rdd.count.toInt)
      assert(newFrameValues.length == newFrameStringColumn.rdd.count.toInt)
      newFrameValues.foreach((r: Row) => assert(r.length == 1)) // each row should only have one column
    }
  }
}