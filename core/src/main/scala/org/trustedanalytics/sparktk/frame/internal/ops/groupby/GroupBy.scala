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
    val groupByColumns: Vector[Column] = frame.frameSchema.copy(columns = groupByColumns).columns

    // run the operation and save results
    val groupByRdd = GroupByAggregationHelper.aggregation(frame, groupByColumns.toList, aggregations)
    new Frame(groupByRdd, groupByRdd.frameSchema)

  }
}


