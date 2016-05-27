package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait RowCountSummarization extends BaseFrame {
  /**
   * Counts all of the rows in the frame.
   *
   * @return The number of rows in the frame.
   */
  def rowCount(): Long = execute[Long](RowCount)
}

/**
 * Number of rows in the current frame
 */
case object RowCount extends FrameSummarization[Long] {
  def work(frame: FrameState): Long = frame.rdd.count()
}

