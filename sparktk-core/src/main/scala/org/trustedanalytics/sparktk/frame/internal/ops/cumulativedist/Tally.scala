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
package org.trustedanalytics.sparktk.frame.internal.ops.cumulativedist

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.{ DataTypes, Column }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait TallyTransform extends BaseFrame {
  /**
   * Count number of times a value is seen.
   *
   * A cumulative count is computed by sequentially stepping through the rows, observing the column values and keeping
   * track of the number of times the specified '''countVal''' has been seen.
   *
   * @param sampleCol The name of the column from which to compute the cumulative count.
   * @param countVal The column value to be used for the counts.
   */
  def tally(sampleCol: String,
            countVal: String): Unit = {
    execute(Tally(sampleCol, countVal))
  }
}

case class Tally(sampleCol: String,
                 countVal: String) extends FrameTransform {
  require(StringUtils.isNotEmpty(sampleCol), "column name for sample is required")
  require(StringUtils.isNotEmpty(countVal), "count value for the sample is required")

  override def work(state: FrameState): FrameState = {
    // run the operation
    val cumulativeDistRdd = CumulativeDistFunctions.cumulativeCount(state, sampleCol, countVal)
    val updatedSchema = state.schema.addColumnFixName(Column(sampleCol + "_tally", DataTypes.float64))
    FrameState(cumulativeDistRdd, updatedSchema)
  }
}