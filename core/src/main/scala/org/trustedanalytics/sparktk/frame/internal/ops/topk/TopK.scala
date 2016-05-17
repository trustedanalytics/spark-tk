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

package org.trustedanalytics.sparktk.frame.internal.ops.topk

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.DataTypes
import org.trustedanalytics.sparktk.frame.{ Frame, FrameSchema, Column }

trait TopKSummarization extends BaseFrame {

  def topK(columnName: String,
           k: Int,
           weightColumn: Option[String]): Frame = {
    execute(TopK(columnName, k, weightColumn))
  }
}

/**
 * Most or least frequent column values.
 *
 * @param columnName The column whose top (or bottom) K distinct values are to be calculated.
 * @param k Number of entries to return (If k is negative, return bottom k).
 * @param weightColumn The column that provides weights (frequencies) for the topK calculation. Must
 *                     contain numerical data.  Default is 1 for all items.
 */
case class TopK(columnName: String,
                k: Int,
                weightColumn: Option[String]) extends FrameSummarization[Frame] {
  require(StringUtils.isNotEmpty(columnName), "column name is required")
  require(k != 0, "k should not be equal to zero")

  override def work(state: FrameState): Frame = {
    val columnIndex = state.schema.columnIndex(columnName)

    // run the operation
    val valueDataType = state.schema.columnDataType(columnName)

    // get the column index and datatype of the
    val weightColumnIndexAndType = weightColumn match {
      case Some(c) =>
        val weightsColumnIndex = state.schema.columnIndex(c)
        Some(weightsColumnIndex, state.schema.column(weightsColumnIndex).dataType)
      case None => None
    }

    val useBottomK = k < 0
    val topRdd = TopKRddFunctions.topK(state.rdd, columnIndex, Math.abs(k), useBottomK, weightColumnIndexAndType)

    val newColumnName = state.schema.getNewColumnName("count")
    val newSchema = FrameSchema(Vector(Column(columnName, valueDataType), Column(newColumnName, DataTypes.float64)))
    new Frame(topRdd, newSchema)
  }
}

