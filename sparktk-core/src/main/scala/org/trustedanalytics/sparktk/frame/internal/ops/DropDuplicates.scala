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
package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }

trait DropDuplicatesTransform extends BaseFrame {
  /**
   * Modify the current frame, removing duplicate rows.
   *
   * Remove data rows which are the same as other rows. The entire row can be checked for duplication, or the search
   * for duplicates can be limited to one or more columns.  This modifies the current frame.
   *
   * @param uniqueColumns Column name(s) to identify duplicates. Default is the entire row is compared.
   */
  def dropDuplicates(uniqueColumns: Option[Seq[String]]): Unit = {
    execute(DropDuplicates(uniqueColumns))
  }
}

case class DropDuplicates(uniqueColumns: Option[Seq[String]]) extends FrameTransform {
  override def work(state: FrameState): FrameState = {
    uniqueColumns match {
      case Some(columns) =>
        val columnNames = state.schema.validateColumnsExist(columns).toVector
        (state: FrameRdd).dropDuplicatesByColumn(columnNames)
      case None =>
        // If no specific columns are specified, return distinct rows across all columns
        FrameState(state.rdd.distinct(), state.schema)
    }
  }
}