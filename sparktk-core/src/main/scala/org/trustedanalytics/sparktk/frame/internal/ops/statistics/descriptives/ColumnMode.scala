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
package org.trustedanalytics.sparktk.frame.internal.ops.statistics.descriptives

import org.trustedanalytics.sparktk.frame.DataTypes.DataType
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait ColumnModeSummarization extends BaseFrame {
  /**
   * Calculate modes of a column.
   *
   * A mode is a data element of maximum weight. All data elements of weight less than or equal to 0 are excluded from
   * the calculation, as are all data elements whose weight is NaN or infinite.  If there are no data elements of
   * finite weight greater than 0, no mode is returned.
   *
   * Because data distributions often have multiple modes, it is possible for a set of modes to be returned. By
   * default, only one is returned, but by setting the optional parameter maxModesReturned, a larger number of modes
   * can be returned.
   *
   * @param dataColumn Name of the column supplying the data.
   * @param weightsColumn Name of the column supplying the weights.
   *                      Default is all items have weight of 1.
   * @param maxModesReturned Maximum number of modes returned. Default is 1.
   * @return The data returned is composed of multiple components:
   *
   *        '''mode''' : ''A mode is a data element of maximum net weight.''<br>
   *        A set of modes is returned. The empty set is returned when the sum of the weights is 0. If the number of
   *        modes is less than or equal to the parameter maxModesReturned, then all modes of the data are returned.
   *        If the number of modes is greater than the maxModesReturned parameter, only the first maxModesReturned
   *        many modes (per a canonical ordering) are returned.
   *
   *        '''weightOfMode''' : ''Weight of a mode.''<br>
   *        If there are no data elements of finite weight greater than 0, the weight of the mode is 0. If no weights
   *        column is given, this is the number of appearances of each mode.
   *
   *        '''totalWeight''' : ''Sum of all weights in the weight column.''<br>
   *        This is the row count if no weights are given.  If no weights column is given, this is the number of rows
   *        in the table with non-zero weight.
   *
   *        '''modeCount''' : ''The number of distinct modes in the data.''<br>
   *          In the case that the data is very multimodal, this number may exceed maxModesReturned.
   */
  def columnMode(dataColumn: String,
                 weightsColumn: Option[String],
                 maxModesReturned: Option[Int]): ColumnModeReturn = {
    execute(ColumnMode(dataColumn, weightsColumn, maxModesReturned))
  }
}

case class ColumnMode(dataColumnName: String,
                      weightsColumn: Option[String],
                      maxModesReturned: Option[Int]) extends FrameSummarization[ColumnModeReturn] {
  require(dataColumnName != null, "data column is required")

  override def work(state: FrameState): ColumnModeReturn = {
    // run the operation and return results
    val dataColumn = state.schema.column(dataColumnName)

    val weightsColumnIndexAndType: Option[(Int, DataType)] = weightsColumn match {
      case None =>
        None
      case Some(weightColumnName) =>
        Some((state.schema.columnIndex(weightsColumn.get), state.schema.columnDataType(weightsColumn.get)))
    }

    val modeCountOption = maxModesReturned

    ColumnStatistics.columnMode(state.schema.columnIndex(dataColumnName),
      dataColumn.dataType,
      weightsColumnIndexAndType,
      modeCountOption,
      state.rdd)
  }
}

/**
 * Mode data for a dataframe column.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 *
 * @param modes A mode is a data element of maximum net weight. A set of modes is returned.
 *             The empty set is returned when the sum of the weights is 0. If the number of modes is <= the parameter
 *             maxNumberOfModesReturned, then all modes of the data are returned.If the number of modes is
 *             > maxNumberOfModesReturned, then only the first maxNumberOfModesReturned many modes
 *             (per a canonical ordering) are returned.
 * @param weightOfMode Weight of the mode. (If no weight column is specified,
 *                     this is the number of appearances of the mode.)
 * @param totalWeight Total weight in the column. (If no weight column is specified, this is the number of entries
 *                    with finite, non-zero weight.)
 * @param modeCount The number of modes in the data.
 */
case class ColumnModeReturn(modes: Any, weightOfMode: Double, totalWeight: Double, modeCount: Long) {
}
