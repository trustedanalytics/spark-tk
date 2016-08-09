package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }
import org.apache.spark.sql.Row

trait CountSummarization extends BaseFrame {
  /**
   * Count the number of rows which meet the given criteria specified by a predicate.
   *
   * @param whereFunction Evaluates the row to a boolean
   * @return Number of rows matching qualifications.
   */
  def count(whereFunction: Row => Boolean): Long = {
    execute(Count(whereFunction))
  }
}

case class Count(whereFunction: Row => Boolean) extends FrameSummarization[Long] {
  require(whereFunction != null, "where predicate is required")
  override def work(state: FrameState): Long = {
    (state: FrameRdd).filter(whereFunction).count()
  }
}

