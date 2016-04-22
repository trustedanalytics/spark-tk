package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CountSummarization extends BaseFrame {

  def count(): Long = execute[Long](Count)
}

case object Count extends FrameSummarization[Long] {

  def work(frame: FrameState): Long = frame.rdd.count()
}

