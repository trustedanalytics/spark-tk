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
package org.trustedanalytics.sparktk.frame.internal.ops.statistics.correlation

import org.trustedanalytics.sparktk.frame.{ FrameSchema, Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.apache.commons.lang.StringUtils

class CorrelationMatrixTest extends TestingSparkContextWordSpec with Matchers {
  "correlation matrix calculations" should {
    "return the correct values" in {

      val inputArray: Array[Array[Double]] = Array(Array(90.0, 60.0, 90.0), Array(90.0, 90.0, 30.0), Array(60.0, 60.0, 60.0), Array(60.0, 60.0, 90.0), Array(30.0, 30.0, 30.0))

      val arrGenericRow: Array[Row] = inputArray.map(row => {
        val temp: Array[Any] = row.map(x => x)
        new GenericRow(temp)
      })

      val rdd = sparkContext.parallelize(arrGenericRow)
      val columnsList = List("col_0", "col_1", "col_2")
      val inputDataColumnNamesAndTypes: Vector[Column] = columnsList.map({ name => Column(name, DataTypes.float64) }).toVector
      val schema = FrameSchema(inputDataColumnNamesAndTypes)
      val frameRdd = new FrameRdd(schema, rdd)
      val result = CorrelationFunctions.correlationMatrix(frameRdd, columnsList).collect()
      result.size shouldBe 3

      result(0) shouldBe Row(1.0, 0.8451542547285167, 0.2988071523335984)
      result(1) shouldBe Row(0.8451542547285167, 1.0, 0.0)
      result(2) shouldBe Row(0.2988071523335984, 0.0, 1.0)
    }
    "return the correct Nan Values" in {
      val inputArrayNan: Array[Array[Double]] = Array(Array(0, 1.0, 4.0, 0.0, -1.0), Array(1, 2.0, 3.0, 0.0, -1.0), Array(2, 3.0, 2.0, 1.0, -1.0), Array(3, 4.0, 1.0, 2.0, -1.0), Array(4, 5.0, 0.0, 2.0, -1.0))
      val arrGenericRow: Array[Row] = inputArrayNan.map(row => {
        val temp: Array[Any] = row.map(x => x)
        new GenericRow(temp)
      })

      val rdd = sparkContext.parallelize(arrGenericRow)
      val columnsList = List("col_0", "col_1", "col_2", "col_3", "col_4")
      val inputDataColumnNamesAndTypes: Vector[Column] = columnsList.map({ name => Column(name, DataTypes.float64) }).toVector
      val schema = FrameSchema(inputDataColumnNamesAndTypes)
      val frameRdd = new FrameRdd(schema, rdd)
      val result = CorrelationFunctions.correlationMatrix(frameRdd, columnsList).collect()
      result.size shouldBe 5

      result(0) shouldBe Row(1.0, 0.9999999999999998, -0.9999999999999998, 0.9486832980505138, Double.NaN)
      result(1) shouldBe Row(0.9999999999999998, 1.0, -0.9999999999999998, 0.9486832980505138, Double.NaN)
      result(2) shouldBe Row(-0.9999999999999998, -0.9999999999999998, 1.0, -0.9486832980505138, Double.NaN)
      result(3) shouldBe Row(0.9486832980505138, 0.9486832980505138, -0.9486832980505138, 1.0, Double.NaN)
      result(4) shouldBe Row(Double.NaN, Double.NaN, Double.NaN, Double.NaN, 1.0)
    }
  }
}
