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
package org.trustedanalytics.sparktk.frame.internal.ops.statistics.covariance

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CovarianceSummarization extends BaseFrame {
  /**
   * Calculate covariance for exactly two columns.
   *
   * @note This method applies only to columns containing numerical data.
   *
   * @param columnNameA The name of the column from which to compute the covariance.
   * @param columnNameB The name of the column from which to compute the covariance.
   * @return Covariance of the two columns.
   */
  def covariance(columnNameA: String,
                 columnNameB: String): Double = {
    execute(Covariance(columnNameA, columnNameB))
  }
}

case class Covariance(columnNameA: String,
                      columnNameB: String) extends FrameSummarization[Double] {
  lazy val dataColumnNames = List(columnNameA, columnNameB)
  require(dataColumnNames.forall(StringUtils.isNotEmpty(_)), "data column names cannot be null or empty.")

  override def work(state: FrameState): Double = {
    state.schema.validateColumnsExist(dataColumnNames)

    CovarianceFunctions.covariance(state, dataColumnNames)
  }

}

