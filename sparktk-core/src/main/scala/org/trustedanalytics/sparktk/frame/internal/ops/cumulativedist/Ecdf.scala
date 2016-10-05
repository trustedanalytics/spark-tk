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
import org.trustedanalytics.sparktk.frame.{ Frame, FrameSchema, Column, DataTypes }
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait EcdfSummarization extends BaseFrame {
  /**
   * Builds a new frame with columns for data and distribution.
   *
   * Generates the :term:`empirical cumulative distribution` for the input column.
   *
   * @param column The name of the input column containing sample
   */
  def ecdf(column: String): Frame = {
    execute(Ecdf(column))
  }
}

case class Ecdf(column: String) extends FrameSummarization[Frame] {
  require(StringUtils.isNotEmpty(column), "column is required")

  override def work(state: FrameState): Frame = {
    val sampleColumn = state.schema.column(column)
    require(sampleColumn.dataType.isNumerical, s"Invalid column ${sampleColumn.name} for ECDF.  Expected a numeric data type, but got ${sampleColumn.dataType}.")
    val ecdfSchema = FrameSchema(Vector(sampleColumn.copy(), Column(sampleColumn.name + "_ecdf", DataTypes.float64)))

    // Create new frame with the result
    new Frame(CumulativeDistFunctions.ecdf(state, sampleColumn), ecdfSchema)
  }
}

