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
package org.trustedanalytics.sparktk.frame.internal.ops.statistics.quantiles

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.{ Column, FrameSchema, DataTypes, Frame }

trait QuantilesSummarization extends BaseFrame {
  /**
   * Calculate quantiles on the given column.
   *
   * @param column the name of the column to calculate quantiles.
   * @param quantiles What is being requested
   * @return A new frame with two columns (''float64''): requested quantiles and their respective values.
   */
  def quantiles(column: String,
                quantiles: List[Double]): Frame = {

    execute(Quantiles(column, quantiles))
  }
}

case class Quantiles(column: String,
                     quantiles: List[Double]) extends FrameSummarization[Frame] {

  override def work(state: FrameState): Frame = {
    val columnIndex = state.schema.columnIndex(column)

    // New schema for the quantiles frame
    val schema = FrameSchema(Vector(Column("Quantiles", DataTypes.float64), Column(column + "_QuantileValue", DataTypes.float64)))

    // return frame with quantile values
    new Frame(QuantilesFunctions.quantiles(state.rdd, quantiles, columnIndex, state.rdd.count()), schema)
  }
}

/**
 * Quantile composing element which contains element's index and its weight
 * @param index element index
 * @param quantileTarget the quantile target that the element can be applied to
 */
case class QuantileComposingElement(index: Long, quantileTarget: QuantileTarget)

