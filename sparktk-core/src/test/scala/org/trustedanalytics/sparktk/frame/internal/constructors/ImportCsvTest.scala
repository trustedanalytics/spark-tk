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

package org.trustedanalytics.sparktk.frame.internal.constructors

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.{ Column, FrameSchema, DataTypes }
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

class ImportCsvTest extends TestingSparkContextWordSpec {

  "ImportCsv" should {
    "Import frame with delimiter = |, header = true, inferSchema = true" in {
      val path = "../integration-tests/datasets/cities.csv"
      val delimiter = "|"
      val header = true
      val inferSchema = true

      val frame = Import.importCsv(sparkContext, path, delimiter, header, inferSchema)

      assert(frame.rdd.count == 20)
      assert(frame.schema.columns.length == 6)
      assert(frame.schema.columnDataType("rank") == DataTypes.int32)
      assert(frame.schema.columnDataType("city") == DataTypes.string)
      assert(frame.schema.columnDataType("population_2013") == DataTypes.int32)
      assert(frame.schema.columnDataType("population_2010") == DataTypes.int32)
      assert(frame.schema.columnDataType("change") == DataTypes.string)
      assert(frame.schema.columnDataType("county") == DataTypes.string)
    }

    "Import frame with delimiter = , and string, int, float, bool, datetime types" in {
      val path = "../integration-tests/datasets/importcsvtest.csv"

      val frame = Import.importCsv(sparkContext, path, ",", header = true, inferSchema = true)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 4)
      assert(frame.schema.columnDataType("string_column") == DataTypes.string)
      assert(frame.schema.columnDataType("integer_column") == DataTypes.int32)
      assert(frame.schema.columnDataType("float_column") == DataTypes.float64)
      assert(frame.schema.columnDataType("datetime_column") == DataTypes.datetime)

      val data = frame.take(1)(0)
      assert(data == Row.fromSeq(Seq("green", 1, 1.0, 1452539454000L)))

    }

    "Import frame without a header row" in {
      val path = "../integration-tests/datasets/noheader.csv"

      val frame = Import.importCsv(sparkContext, path, ",", header = false, inferSchema = true)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 4)
      assert(frame.schema.columnDataType("C0") == DataTypes.string)
      assert(frame.schema.columnDataType("C1") == DataTypes.int32)
      assert(frame.schema.columnDataType("C2") == DataTypes.float64)
      assert(frame.schema.columnDataType("C3") == DataTypes.datetime)
    }

    "Import frame without inferring schema and no header row" in {
      val path = "../integration-tests/datasets/noheader.csv"

      val frame = Import.importCsv(sparkContext, path, ",", header = false, inferSchema = false)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 4)
      // When no schema is inferred or specified, they are all strings
      for (i <- 0 until 4) {
        assert(frame.schema.columnDataType("C" + i.toString) == DataTypes.string)
      }
    }

    "Import frame with a header row, but no inferred schema" in {
      val path = "../integration-tests/datasets/importcsvtest.csv"

      val frame = Import.importCsv(sparkContext, path, ",", header = true, inferSchema = false)

      assert(frame.rdd.count == 10)
      assert(frame.schema.columns.length == 4)
      // When no schema is inferred or specified, they are all strings, but since
      // we have a header, they should have column names
      assert(frame.schema.columnDataType("string_column") == DataTypes.string)
      assert(frame.schema.columnDataType("integer_column") == DataTypes.string)
      assert(frame.schema.columnDataType("float_column") == DataTypes.string)
      assert(frame.schema.columnDataType("datetime_column") == DataTypes.string)
    }

    "Import frame with unsupport data types" in {
      val path = "../integration-tests/datasets/unsupported_types.csv"

      intercept[Exception] {
        // Frame contains booleans, which aren't supported, so this should cause an exception when inferring the schema
        Import.importCsv(sparkContext, path, ",", inferSchema = true)
      }

      val schema = FrameSchema(Vector(Column("id", DataTypes.int32),
        Column("name", DataTypes.string),
        Column("bool", DataTypes.string),
        Column("date", DataTypes.string)))

      // Specify the schema to treat booleans as strings, so this should pass
      val frame = Import.importCsv(sparkContext, path, ",", inferSchema = false, schema = Some(schema))
      assert(frame.rowCount() == 5)
    }

    "Import multiple csv files" in {
      val path = "../integration-tests/datasets/movie-part*.csv"

      val frame = Import.importCsv(sparkContext, path, ",", header = true, inferSchema = true)

      assert(frame.rowCount() == 20)
      assert(frame.schema == FrameSchema(Vector(Column("user", DataTypes.int32),
        Column("vertex_type", DataTypes.string),
        Column("movie", DataTypes.int32),
        Column("weight", DataTypes.int32),
        Column("edge_type", DataTypes.string))))
    }

    "Import csv with missing values" in {
      val path = "../integration-tests/datasets/missing_values.csv"
      val frame = Import.importCsv(sparkContext, path, ",", header = false, inferSchema = true)
      assert(frame.rowCount() == 5)
      assert(frame.schema == FrameSchema(Vector(Column("C0", DataTypes.string),
        Column("C1", DataTypes.int32),
        Column("C2", DataTypes.int32),
        Column("C3", DataTypes.int32),
        Column("C4", DataTypes.int32),
        Column("C5", DataTypes.float64))))
      val data = frame.take(frame.rowCount().toInt)
      val expectedData: Array[Row] = Array(
        new GenericRow(Array[Any]("1", 2, null, 4, 5, null)),
        new GenericRow(Array[Any]("1", 2, 3, null, null, 2.5)),
        new GenericRow(Array[Any]("2", 1, 3, 4, 5, null)),
        new GenericRow(Array[Any]("dog", 20, 30, 40, 50, 60.5)),
        new GenericRow(Array[Any]("", null, 13, 14, 15, 16.5))
      )
      assert(data.sameElements(expectedData))
    }
  }

}
