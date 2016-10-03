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
package org.trustedanalytics.sparktk.frame.internal.ops.timeseries

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.apache.spark.mllib.linalg.{ DenseVector, Vector => SparkVector, Matrix }

trait TimeSeriesBreuschPaganTestSummarization extends BaseFrame {
  /**
   * Peforms the Breusch-Pagan test for heteroskedasticity.
   *
   * @param residuals Name of the column that contains residual (y) values
   * @param factors Name of the column(s) that contain factors (x) values
   * @return The Breusch-Pagan test statistic and p-value
   */
  def timeSeriesBreuschPaganTest(residuals: String,
                                 factors: Seq[String]): BpTestReturn = {
    execute(TimeSeriesBreuschPaganTest(residuals, factors))
  }
}

case class TimeSeriesBreuschPaganTest(residuals: String,
                                      factors: Seq[String]) extends FrameSummarization[BpTestReturn] {
  require(StringUtils.isNotEmpty(residuals), "residuals name must not be null or empty.")
  require(factors != null && factors.nonEmpty, "factors string must not be null or empty.")

  override def work(state: FrameState): BpTestReturn = {
    val (vector, matrix) = TimeSeriesFunctions.getSparkVectorYAndXFromFrame(new FrameRdd(state.schema, state.rdd), residuals, factors)

    val result = TimeSeriesStatisticalTests.bptest(vector, matrix)
    BpTestReturn(result._1, result._2)
  }
}

/**
 * Return value for the Breusch-Pagan Test
 * @param testStat Breusch-Pagan test statistic
 * @param pValue p-value
 */
case class BpTestReturn(testStat: Double, pValue: Double)