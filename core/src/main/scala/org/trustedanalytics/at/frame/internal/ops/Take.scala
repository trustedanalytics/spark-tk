package org.trustedanalytics.at.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.at.frame.internal.{ FrameState, FrameSummarization, BaseFrame }

trait TakeSummarization extends BaseFrame {

  def take(n: Int): scala.Array[Row] = {
    execute(Take(n))
  }

  //def takePython(n: Int): scala.Array[Any]Scala.Array[Byte]]
}

/**
 * @param n - number of rows to take
 */
case class Take(n: Int) extends FrameSummarization[scala.Array[Row]] {

  override def work(state: FrameState): scala.Array[Row] = {
    state.rdd.take(n)
  }
}
