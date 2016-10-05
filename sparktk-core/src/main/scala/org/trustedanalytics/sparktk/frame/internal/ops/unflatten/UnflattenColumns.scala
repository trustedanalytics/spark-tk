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
package org.trustedanalytics.sparktk.frame.internal.ops.unflatten

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait UnflattenColumnsTransform extends BaseFrame {
  /**
   * Compacts data from multiple rows based on the cell data.
   *
   * Groups together cells in all columns (less the composite key) using "," as string delimiter. The original rows
   * are deleted.  The grouping takes place based on a composite key created from cell values. The column data types
   * are changed to string.
   *
   * @param columns Name of the column(s) to be used as keys for unflattening
   * @param delimiter Separator for the data in the result columns.  Default is comma (,).
   */
  def unflattenColumns(columns: List[String],
                       delimiter: String = ","): Unit = {
    execute(UnflattenColumns(columns, delimiter))
  }
}

case class UnflattenColumns(columns: List[String],
                            delimiter: String) extends FrameTransform {
  require(columns != null && columns.nonEmpty, "column list is required for key")
  columns.foreach(x => require(StringUtils.isNotBlank(x), "non empty column names are required."))
  // Parameter validation
  override def work(state: FrameState): FrameState = {
    val schema = state.schema
    val compositeKeyNames = columns
    val compositeKeyIndices = compositeKeyNames.map(schema.columnIndex)

    // run the operation
    val targetSchema = UnflattenColumnsFunctions.createTargetSchema(schema, compositeKeyNames)
    val initialRdd = (state: FrameRdd).groupByRows(row => row.values(compositeKeyNames.toVector))
    val resultRdd = UnflattenColumnsFunctions.unflattenRddByCompositeKey(compositeKeyIndices, initialRdd, targetSchema, delimiter)

    FrameState(resultRdd, targetSchema)
  }
}