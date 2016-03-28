package org.trustedanalytics.at.frame.internal.ops

import org.trustedanalytics.at.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait CountTrait extends BaseFrame {

  def count(): Long = execute[Long](Count)
}

case object Count extends FrameSummarization[Long] {

  def work(frame: FrameState): Long = frame.rdd.count()
}

