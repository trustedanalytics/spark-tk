package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait TakeSummarization extends BaseFrame {
  /**
   * Get data subset.
   *
   * @param n Number of rows to take
   * @return Array of rows
   */
  def take(n: Int): scala.Array[Row] = {
    execute(Take(n))
  }

  //def takePython(n: Int): scala.Array[Any]Scala.Array[Byte]]
}

case class Take(n: Int) extends FrameSummarization[scala.Array[Row]] {

  override def work(state: FrameState): scala.Array[Row] = {
    state.rdd.take(n)
  }
}
