package org.trustedanalytics.sparktk.frame.internal.ops

import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameTransform, BaseFrame }
import org.trustedanalytics.sparktk.frame.Frame

trait AppendFrameTransform extends BaseFrame {
  /**
   * Adds more data to the current frame.
   *
   * @param frame Frame of data to append
   */
  def append(frame: Frame): Unit = {
    execute(Append(frame))
  }
}

case class Append(frame: Frame) extends FrameTransform {
  require(frame != null, "frame parameter is required.")

  override def work(state: FrameState): FrameState = {
    (state: FrameRdd).union(new FrameRdd(frame.schema, frame.rdd))
  }
}