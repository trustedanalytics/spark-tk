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

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CorrelationSummarization extends BaseFrame {
  /**
   * Calculate correlation for two columns of the current frame.
   *
   * @note This method applies only to columns containing numerical data.
   *
   * @param columnNameA The name of the column to compute the correlation.
   * @param columnNameB The name of the column to compute the correlation.
   * @return Pearson correlation coefficient of the two columns.
   */
  def correlation(columnNameA: String,
                  columnNameB: String): Double = {
    execute(Correlation(columnNameA, columnNameB))
  }
}

case class Correlation(columnNameA: String, columnNameB: String) extends FrameSummarization[Double] {
  lazy val dataColumnNames = List(columnNameA, columnNameB)
  require(dataColumnNames.forall(StringUtils.isNotEmpty(_)), "data column names cannot be null or empty.")

  override def work(state: FrameState): Double = {
    state.schema.validateColumnsExist(dataColumnNames)

    // Calculate correlation
    CorrelationFunctions.correlation(state, dataColumnNames)
  }

}

