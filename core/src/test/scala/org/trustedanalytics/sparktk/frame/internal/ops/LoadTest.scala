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

import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class LoadTest extends TestingSparkContextWordSpec {

  "LoadFromCsv" should {
    "Load frame with delimiter = |, header = true, inferSchema = true" in {
      val path = "../integration-tests/datasets/cities.csv"
      val delimiter = "|"
      val header = true
      val inferSchema = true

      val frame = Load.loadFromCsv(path, delimiter, header, inferSchema, sparkContext)

      assert(frame.rdd.count == 20)
      assert(frame.schema.columns.length == 6)
      assert(frame.schema.columnDataType("rank") == DataTypes.int32)
      assert(frame.schema.columnDataType("city") == DataTypes.string)
      assert(frame.schema.columnDataType("population_2013") == DataTypes.int32)
      assert(frame.schema.columnDataType("population_2010") == DataTypes.int32)
      assert(frame.schema.columnDataType("change") == DataTypes.string)
      assert(frame.schema.columnDataType("county") == DataTypes.string)
    }

    "Load frame with delimiter = , and string, int, float, bool, datetime types" in {
      val path = "../integration-tests/datasets/csvloadtest.csv"

      val frame = Load.loadFromCsv(path, ",", true, true, sparkContext)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 5)
      assert(frame.schema.columnDataType("string_column") == DataTypes.string)
      assert(frame.schema.columnDataType("integer_column") == DataTypes.int32)
      assert(frame.schema.columnDataType("float_column") == DataTypes.float64)
      assert(frame.schema.columnDataType("bool_column") == DataTypes.int32) // We don't have a bool type
      assert(frame.schema.columnDataType("datetime_column") == DataTypes.string) // We don't have a data/time type
    }

    "Load frame without a header row" in {
      val path = "../integration-tests/datasets/noheader.csv"

      val frame = Load.loadFromCsv(path, ",", false, true, sparkContext)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 5)
      assert(frame.schema.columnDataType("C0") == DataTypes.string)
      assert(frame.schema.columnDataType("C1") == DataTypes.int32)
      assert(frame.schema.columnDataType("C2") == DataTypes.float64)
      assert(frame.schema.columnDataType("C3") == DataTypes.int32) // We don't have a bool type
      assert(frame.schema.columnDataType("C4") == DataTypes.string) // We don't have a data/time type
    }

    "Load frame without inferring schema and no header row" in {
      val path = "../integration-tests/datasets/noheader.csv"

      val frame = Load.loadFromCsv(path, ",", false, false, sparkContext)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 5)
      // When no schema is inferred or specified, they are all strings
      for (i <- 0 until 5) {
        assert(frame.schema.columnDataType("C" + i.toString) == DataTypes.string)
      }
    }

    "Load frame with a header row, but no inferred schema" in {
      val path = "../integration-tests/datasets/csvloadtest.csv"

      val frame = Load.loadFromCsv(path, ",", true, false, sparkContext)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 5)
      // When no schema is inferred or specified, they are all strings, but since
      // we have a header, they should have column names
      assert(frame.schema.columnDataType("string_column") == DataTypes.string)
      assert(frame.schema.columnDataType("integer_column") == DataTypes.string)
      assert(frame.schema.columnDataType("float_column") == DataTypes.string)
      assert(frame.schema.columnDataType("bool_column") == DataTypes.string)
      assert(frame.schema.columnDataType("datetime_column") == DataTypes.string)
    }
  }

}
