package org.trustedanalytics.sparktk.frame.internal.ops

import org.apache.spark.sql.Row
import org.trustedanalytics.sparktk.frame.internal._
import org.trustedanalytics.sparktk.frame.internal.rdd.FrameRdd

trait DropRowsTransform extends BaseFrame {
  /**
   * Drop all rows which satisfy a predicate.
   *
   * Modifies the current frame to drop defined rows and save everything else.
   *
   * @note
   *       1. The rowFunction must return a boolean
   * @param rowFunction map function which decides whether to drop a row or not
   */
  def dropRows(rowFunction: Row => Boolean): Unit = {
    execute(DropRows(rowFunction))
  }

}

case class DropRows(rowFunction: Row => Boolean) extends FrameTransform {

  require(rowFunction != null, "predicate is required")

  def invertedPredicate(row: Row): Boolean = {
    !rowFunction(row)
  }

  override def work(state: FrameState): FrameState = {
    val dropRowsRdd = (state: FrameRdd).filter(invertedPredicate)
    FrameState(dropRowsRdd, state.schema)
  }
}
