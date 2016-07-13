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

package org.trustedanalytics.sparktk.frame.internal.ops.timeseries

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

trait AugmentedDickeyFullerTestSummarization extends BaseFrame {

  /**
   * Performs the Augmented Dickey-Fuller (ADF) Test, which tests the null hypothesis of whether a unit root is present
   * in a time series sample. The test statistic that is returned in a negative number.  The lower the value, the
   * stronger the rejection of the hypothesis that there is a unit root at some level of confidence.
   *
   * @param tsColumn Name of the column that contains the time series values to use with the ADF test.
   * @param maxLag The lag order to calculate the test statistic.
   * @param regression The method of regression that was used. Following MacKinnon's notation, this can be "c" for
   *                   constant, "nc" for no constant, "ct" for constant and trend, and "ctt" for constant, trend,
   *                   and trend-squared.
   * @return Object that contains the ADF test statistic and p-value
   */
  def augmentedDickeyFullerTest(tsColumn: String,
                                maxLag: Int,
                                regression: String = "c"): AdfTestReturn = {
    execute(AugmentedDickeyFullerTest(tsColumn, maxLag, regression))
  }
}

case class AugmentedDickeyFullerTest(tsColumn: String,
                                     maxLag: Int,
                                     regression: String) extends FrameSummarization[AdfTestReturn] {
  require(StringUtils.isNotEmpty(tsColumn), "tsColumn name must not be null or empty.")
  require(StringUtils.isNotEmpty(regression), "regression string must not be null or empty.")

  override def work(state: FrameState): AdfTestReturn = {
    val tsVector = TimeSeriesFunctions.getVectorFromFrame(state, tsColumn)
    val dftResult = TimeSeriesStatisticalTests.adftest(tsVector, maxLag, regression)
    return AdfTestReturn(dftResult._1, dftResult._2)
  }
}

/**
 *
 * Return value for the AugmentedDickeyFullerTest
 *
 * @param testStat Augmented Dickey–Fuller (ADF) statistic
 * @param pValue p-value
 */
case class AdfTestReturn(testStat: Double, pValue: Double)

