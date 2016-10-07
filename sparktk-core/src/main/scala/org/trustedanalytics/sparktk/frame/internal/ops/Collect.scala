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
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CollectSummarization extends BaseFrame {
  /**
   * Collect all the frame data locally
   *
   * @param columns Name of columns; if specified, only data from these columns will be collected
   * @return Array of rows
   */
  def collect(columns: Option[Seq[String]] = None): scala.Array[Row] = {
    execute(Collect(columns))
  }
}

case class Collect(columns: Option[Seq[String]]) extends FrameSummarization[scala.Array[Row]] {

  override def work(state: FrameState): scala.Array[Row] = {
    columns match {
      case None => state.rdd.collect()
      case Some(cols) =>
        val indices = state.schema.columnIndices(cols)
        state.rdd.map(row => new GenericRow(indices.map(i => row(i)).toArray).asInstanceOf[Row]).collect()
    }
  }
}