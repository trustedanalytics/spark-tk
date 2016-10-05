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
package org.trustedanalytics.sparktk.frame.internal.ops.groupby

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.{ Column, Frame, Schema }
import org.trustedanalytics.sparktk.frame.internal.{ BaseFrame, FrameState, FrameSummarization }

trait GroupBySummarization extends BaseFrame {

  /**
   * Summarized Frame with Aggregations.
   *
   * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, Mean, Sum, Stdev, ...).
   *
   * @param groupByColumns list of columns to group on
   * @param aggregations list of lists contains aggregations to perform
   *                     Each inner list contains below three strings
   *                     function: Name of aggregation function (e.g., count, sum, variance)
   *                     columnName: Name of column to aggregate
   *                     newColumnName: Name of new column that stores the aggregated results
   * @return Summarized Frame
   */
  def groupBy(groupByColumns: List[String], aggregations: List[GroupByAggregationArgs]) = {
    execute(GroupBy(groupByColumns, aggregations))
  }
}

case class GroupBy(groupByColumns: List[String], aggregations: List[GroupByAggregationArgs]) extends FrameSummarization[Frame] {

  require(groupByColumns != null, "group_by columns is required")
  require(aggregations != null, "aggregation list is required")

  override def work(state: FrameState): Frame = {
    val frame: FrameRdd = state
    val groupByColumnList: Iterable[Column] = frame.frameSchema.columns(columnNames = groupByColumns)

    // run the operation and save results
    val groupByRdd = GroupByAggregationHelper.aggregation(frame, groupByColumnList.toList, aggregations)
    new Frame(groupByRdd, groupByRdd.frameSchema)

  }
}

