package org.trustedanalytics.at.frame.ops

import org.trustedanalytics.at.frame.{ FrameState, FrameSummarization, BaseFrame }

trait CountTrait extends BaseFrame {

  def count(): Long = execute[Long](Count)
}

case object Count extends FrameSummarization[Long] {

  def work(frame: FrameState): Long = frame.rdd.count()
}

