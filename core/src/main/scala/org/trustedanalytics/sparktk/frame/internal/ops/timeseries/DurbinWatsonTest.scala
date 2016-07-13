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

trait DurbinWatsonTestSummarization extends BaseFrame {
  /**
   * Computes the Durbin-Watson test statistic used to determine the presence of serial correlation in the residuals.
   * Serial correlation can show a relationship between values separated from each other by a given time lag. A value
   * close to 0.0 gives evidence for positive serial correlation, a value close to 4.0 gives evidence for negative
   * serial correlation, and a value close to 2.0 gives evidence for no serial correlation.
   *
   * @param residuals Name of the column that contains the residual values
   * @return The Durbin-Watson test statistic
   */
  def durbinWatsonTest(residuals: String): Double = {
    execute(DurbinWatsonTest(residuals))
  }
}

case class DurbinWatsonTest(residuals: String) extends FrameSummarization[Double] {
  require(StringUtils.isNotEmpty(residuals), "residuals must not be null or empty.")

  override def work(state: FrameState): Double = {
    return TimeSeriesStatisticalTests.dwtest(TimeSeriesFunctions.getVectorFromFrame(state, residuals))
  }
}

