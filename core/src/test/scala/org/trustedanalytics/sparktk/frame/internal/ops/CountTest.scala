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
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.frame.{ Frame, Column, DataTypes, FrameSchema }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class CountTest extends TestingSparkContextWordSpec with Matchers {

  "frame count" should {
    "count based on a math equation" in {
      val schema = FrameSchema(Vector(Column("num", DataTypes.int32)))
      val rows = FrameRdd.toRowRDD(schema, sparkContext.parallelize((1 to 100).map(i => Array[Any](i))).repartition(3))
      val frame = new Frame(rows, schema)

      def evenNumbers(row: Row): Boolean = {
        row.getInt(schema.columnIndex("num")) % 2 == 0
      }

      assert(frame.count(evenNumbers) == 50)
    }

    "count based on a string filter" in {
      val schema = FrameSchema(Vector(Column("name", DataTypes.string)))
      val rows = FrameRdd.toRowRDD(schema,
        sparkContext.parallelize(List("sam", "bob", "lisa", "joe", "mary", "albert", "tom").map(i => Array[Any](i))))
      val frame = new Frame(rows, schema)

      def containsA(row: Row): Boolean = {
        row.getString(schema.columnIndex("name")).contains("a")
      }

      assert(frame.count(containsA) == 4)
    }

    "count with using multiple rows" in {
      val schema = FrameSchema(Vector(Column("a", DataTypes.int32), Column("b", DataTypes.int32)))
      val rows = FrameRdd.toRowRDD(schema,
        sparkContext.parallelize(List(Array[Any](0, -1),
          Array[Any](10, 20),
          Array[Any](0, 15),
          Array[Any](-5, 10),
          Array[Any](1, -1),
          Array[Any](5, -10))))
      val frame = new Frame(rows, schema)

      def sumGreaterThanZero(row: Row): Boolean = {
        val a = row.getInt(schema.columnIndex("a"))
        val b = row.getInt(schema.columnIndex("b"))
        (a + b) > 0
      }

      assert(frame.count(sumGreaterThanZero) == 3)
    }

    "return zero when nothing matches" in {
      val schema = FrameSchema(Vector(Column("num", DataTypes.int32)))
      val rows = FrameRdd.toRowRDD(schema, sparkContext.parallelize((1 to 10).map(i => Array[Any](i))).repartition(3))
      val frame = new Frame(rows, schema)

      def greaterThan10(row: Row): Boolean = {
        row.getInt(schema.columnIndex("num")) > 10
      }

      assert(frame.count(greaterThan10) == 0)
    }

    "throw an exception when passed null" in {
      val schema = FrameSchema(Vector(Column("num", DataTypes.int32)))
      val rows = FrameRdd.toRowRDD(schema, sparkContext.parallelize((1 to 10).map(i => Array[Any](i))).repartition(3))
      val frame = new Frame(rows, schema)

      intercept[IllegalArgumentException] {
        frame.count(null)
      }
    }
  }
}