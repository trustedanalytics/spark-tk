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

package org.trustedanalytics.sparktk.frame.internal.constructors

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.trustedanalytics.sparktk.frame.{ FrameSchema, Frame, Column, DataTypes }

class FrameInitTest extends TestingSparkContextWordSpec {
  "Frame init" should {
    "infer the schema (int and string) if no schema is provided" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](1, "one")),
        new GenericRow(Array[Any](2, "two")),
        new GenericRow(Array[Any](3, "three")),
        new GenericRow(Array[Any](4, "four")),
        new GenericRow(Array[Any](5, "five"))
      )
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, null)
      assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.int32), Column("C1", DataTypes.string))))
    }

    "infer the schema when there is a mix of int, floats, and strings" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](1, 1, 2.5)),
        new GenericRow(Array[Any](2.2, "2", 2)),
        new GenericRow(Array[Any](3, "three", 7)),
        new GenericRow(Array[Any](4, 2.3, 8.5)),
        new GenericRow(Array[Any](5, "five", "seven"))
      )
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, null)
      assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.float64),
        Column("C1", DataTypes.string),
        Column("C2", DataTypes.string))))
    }

    "infer the schema with vectors, if no schema is provided" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](List[Int](1, 2, 3))),
        new GenericRow(Array[Any](List[Int](4, 5, 6))),
        new GenericRow(Array[Any](List[Int](7, 8, 9)))
      )
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, null)
      assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.vector(3)))))
      val data = frame.take(frame.rowCount().toInt)
      for ((frameRow, originalRow) <- (data.zip(rows))) {
        assert(frameRow.equals(originalRow))
      }
    }

    "infer the schema, with missing values" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](1, 2, null)),
        new GenericRow(Array[Any](4, null, null)),
        new GenericRow(Array[Any](7, 8, null))
      )
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, null)
      assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.int32),
        Column("C1", DataTypes.int32),
        Column("C2", DataTypes.int32))))
      val data = frame.take(frame.rowCount().toInt)
      assert(data.sameElements(rows))
    }

    "throw an exception when vectors aren't all the same length" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](List[Int](1, 2, 3))),
        new GenericRow(Array[Any](List[Int](4, 5, 6))),
        new GenericRow(Array[Any](List[Int](7, 8, 9, 10)))
      )
      val rdd = sparkContext.parallelize(rows)

      intercept[RuntimeException] {
        val frame = new Frame(rdd, null)
        assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.vector(3)))))
        val data = frame.take(frame.rowCount().toInt)
      }
    }

    "test schema validation" in {
      val rows: Array[Row] = Array(
        new GenericRow(Array[Any](1)),
        new GenericRow(Array[Any](2)),
        new GenericRow(Array[Any](3)),
        new GenericRow(Array[Any](4)),
        new GenericRow(Array[Any](5.1))
      )
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, null, true)
      assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.float64))))
      // All values should be double
      assert(frame.take(frame.rowCount.toInt).forall(row => row.get(0).isInstanceOf[Double]))
    }

    "include missing values, if data past the first 100 rows does not match the schema, when validation is enabled" in {
      val intRows: Array[Row] = Array.fill(100) { new GenericRow(Array[Any](1)) }
      val floatRows: Array[Row] = Array.fill(20) { new GenericRow(Array[Any]("a")) }
      val rows: Array[Row] = intRows ++ floatRows
      val rdd = sparkContext.parallelize(rows)
      val frame = new Frame(rdd, null, validateSchema = true)
      val data = frame.take(frame.rowCount.toInt)
      // check for missing values
      data.slice(100, data.length).foreach(r => assert(r.get(0) == null))
      assert(frame.validationReport.isDefined == true)
      assert(20 == frame.validationReport.get.numBadValues)
    }

    "no exception if data past the first 100 rows does not match the schema, if validation is disabled" in {
      val intRows: Array[Row] = Array.fill(100) { new GenericRow(Array[Any](1)) }
      val floatRows: Array[Row] = Array.fill(20) { new GenericRow(Array[Any]("a")) }
      val rows: Array[Row] = intRows ++ floatRows
      val rdd = sparkContext.parallelize(rows)

      val frame = new Frame(rdd, null, validateSchema = false)
      assert(frame.take(frame.rowCount.toInt).length == frame.rowCount)
      assert(frame.validationReport.isDefined == false)
    }
  }
}
