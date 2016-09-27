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

package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.DataTypes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.sparktk.testutils.TestingSparkContextWordSpec

/**
 * Exercises the column statistics functions. Primarily checks that correct column indices and options are piped
 * through to the underlying statistics engines. Thorough evaluation of the statistical operations is done by the
 * tests for the respective statistics engines.
 */
class ColumnStatisticsITest extends TestingSparkContextWordSpec with Matchers {

  trait ColumnStatisticsTest {

    val epsilon = 0.000000001

    // Input data
    val row0 = Row("A", 1, 2.0f, 2, 3, 1.0f, 0, 0)
    val row1 = Row("B", 1, 2.0f, 1, 3, 2.0f, 0, 0)
    val row2 = Row("C", 1, 2.0f, 3, 2, 0.0f, 10, 0)
    val row3 = Row("D", 1, 2.0f, 6, 1, 1.0f, 0, 0)
    val row4 = Row("E", 1, 2.0f, 7, 1, 2.0f, 0, 0)

    val rowRDD: RDD[Row] = sparkContext.parallelize(List(row0, row1, row2, row3, row4))
  }

  "ColumnStatistics.columnMode" should {
    "with no net weight, return none as json" in new ColumnStatisticsTest() {
      val testMode = ColumnStatistics.columnMode(0, DataTypes.string, Some(7, DataTypes.int32), None, rowRDD)

      testMode.modes shouldBe Array.empty[String]
    }

    "support weighted mode" in new ColumnStatisticsTest() {

      val testMode = ColumnStatistics.columnMode(0, DataTypes.string, Some(3, DataTypes.int32), None, rowRDD)

      testMode.modes shouldBe Array("E")
    }
  }

  "ColumnStatistics.columnSummaryStatistics" should {
    "support unweighted summary statistics" in new ColumnStatisticsTest() {

      val stats: ColumnSummaryStatisticsReturn = ColumnStatistics.columnSummaryStatistics(2,
        DataTypes.float32,
        None,
        None,
        rowRDD,
        usePopulationVariance = false)

      Math.abs(stats.mean - 2.0) should be < epsilon
    }

    "support weighted summary statistics" in new ColumnStatisticsTest() {

      val stats: ColumnSummaryStatisticsReturn =
        ColumnStatistics.columnSummaryStatistics(5, DataTypes.float32, Some(4), Some(DataTypes.int32), rowRDD, usePopulationVariance = false)

      Math.abs(stats.mean - 1.2) should be < epsilon
    }
  }

  "ColumnStatistics.columnMedian" should {
    "support unweighted float median" in new ColumnStatisticsTest() {
      val median = ColumnStatistics.columnMedian(2, DataTypes.float32, None, rowRDD)

      median shouldBe 2.0
    }

    "support weighted float median" in new ColumnStatisticsTest() {
      val median =
        ColumnStatistics.columnMedian(5, DataTypes.float32, Some(6, DataTypes.int32), rowRDD)

      median shouldBe 0.0
    }

    "support unweighted integer median" in new ColumnStatisticsTest() {
      val median =
        ColumnStatistics.columnMedian(4, DataTypes.int32, None, rowRDD)

      median shouldBe 2
    }

    "support weighted integer median" in new ColumnStatisticsTest() {
      val median =
        ColumnStatistics.columnMedian(4, DataTypes.int32, Some(1, DataTypes.int32), rowRDD)

      median shouldBe 2
    }

    "with no net weights should return none" in new ColumnStatisticsTest() {
      val median = ColumnStatistics.columnMedian(0, DataTypes.string, Some(7, DataTypes.int32), rowRDD)

      median shouldBe None
    }
  }
}
