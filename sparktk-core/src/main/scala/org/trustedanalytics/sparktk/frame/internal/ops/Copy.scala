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

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.trustedanalytics.sparktk.frame.Frame

trait CopySummarization extends BaseFrame {
  /**
   * Copies specified columns into a new Frame object, optionally renaming them and/or filtering them.
   *
   * @param columns Optional dictionary of column names to include in the copy and target names.  The default
   *                behavior is that all columns will be included in the frame that is returned.
   * @param where Optional function to filter the rows that are included.  The default behavior is that
   *              all rows will be included in the frame that is returned.
   * @return New frame object.
   */
  def copy(columns: Option[Map[String, String]] = None,
           where: Option[Row => Boolean] = None): Frame = {
    execute(Copy(columns, where))
  }
}

case class Copy(columns: Option[Map[String, String]] = None,
                where: Option[Row => Boolean]) extends FrameSummarization[Frame] {
  override def work(state: FrameState): Frame = {

    val finalSchema = columns.isDefined match {
      case true => state.schema.copySubsetWithRename(columns.get)
      case false => state.schema
    }

    var filteredRdd = if (where.isDefined) state.rdd.filter(where.get) else state.rdd

    if (columns.isDefined) {
      val frameRdd = new FrameRdd(state.schema, filteredRdd)
      filteredRdd = frameRdd.selectColumnsWithRename(columns.get)
    }

    new Frame(filteredRdd, finalSchema)
  }
}

