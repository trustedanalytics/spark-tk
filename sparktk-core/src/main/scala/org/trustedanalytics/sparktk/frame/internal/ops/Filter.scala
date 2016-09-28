package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait FilterTransform extends BaseFrame {
  /**
   * Select all rows which satisfy a predicate.
   *
   * Modifies the current frame to save defined rows and delete everything else.
   *
   * @note
   *       1. The rowFunction must return a boolean
   *
   * @param rowFunction map function which decides whether to keep a row or drop
   */
  def filter(rowFunction: Row => Boolean): Unit = {
    execute(Filter(rowFunction))
  }

}

case class Filter(rowFunction: Row => Boolean) extends FrameTransform {

  require(rowFunction != null, "predicate is required")

  override def work(state: FrameState): FrameState = {
    val filterRdd = (state: FrameRdd).filter(rowFunction)
    FrameState(filterRdd, state.schema)
  }
}